package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"my-upstox-client/marketdatafeedpb"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var accessToken string = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2VUJUN1ciLCJqdGkiOiI2ODE1YWQzYmM4MjAxZTA1NzU3YmRlY2EiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc0NjI1MTA2NywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzQ2MzA5NjAwfQ.TEdZSqfVTQgOdKO1onCPbvCY-GeTBOMV4OhqXel1pbg"
var amount float64
var lev int

type Candle struct {
	Open      float64
	High      float64
	Low       float64
	Close     float64
	StartTime time.Time
}

var candleData = make(map[string]*Candle)
var candles = make(map[string]*Candle)

var instrumentData = make(map[string]InstrumentDetails)

type InstrumentDetails struct {
	SMA    float64 `json:"sma"`
	Trend  string  `json:"trend"`
	Symbol string  `json:"symbol"`
	UL     float64 `json:"upperlim"`
	SL     float64 `json:"lowerlim"`
}

var instrumentKeys []string

var (
	bullish    = &SafeSlice{}
	placed     = &SafeSlice{}
	insKeys    = &SafeSlice{}
	closetosma = &SafeSlice{}
	onceplaced = &SafeSlice{}
)

var order10 = make(map[string]Order10)

type Order10 struct {
	SL       float64 `json:"sl"`
	USL      float64 `json:"usl"`
	SLGAP    float64 `json:"slgap"`
	OPRICE   float64 `json:"oprice"`
	ORDERID  string  `json:"orderid"`
	OTYPE    string  `json:"otype"`
	QUANTITY int     `json:"quantity"`
}

type Response struct {
	Status string `json:"status"`
	Data   struct {
		OrderID string `json:"order_id"`
	} `json:"data"`
}

var stockChannel1 = make(chan struct {
	Instrument string
	Price      float64
}, 10000) // Large buffer to avoid blocking

var stockChannel2 = make(chan struct {
	Instrument string
	Price      float64
}, 10000) // Large buffer to avoid blocking

type SafeSlice struct {
	items []string
	mux   sync.RWMutex
}

func (s *SafeSlice) Contains(item string) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()
	for _, v := range s.items {
		if v == item {
			return true
		}
	}
	return false
}

func (s *SafeSlice) Append(item string) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.items = append(s.items, item)
}

func getIntervalStart(t time.Time) time.Time {
	minutes := (t.Minute() / 5) * 5
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minutes, 0, 0, t.Location())
}

const (
	authURL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
)

type MarketDataClient struct {
	conn      *websocket.Conn
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	connected bool
}

func NewMarketDataClient() *MarketDataClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &MarketDataClient{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *MarketDataClient) startPingLoop() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.connected {
				c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
			c.mu.Unlock()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *MarketDataClient) Connect() error {
	wsURL, err := getWebSocketURL()
	if err != nil {
		return fmt.Errorf("failed to get WebSocket URL: %v", err)
	}

	log.Printf("Connecting to WebSocket endpoint: %s", wsURL)

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second, // Reduce from 1800s
		NetDialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second, // TCP keepalive
		}).DialContext,
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+accessToken)
	headers.Add("Accept", "*/*")

	conn, _, err := dialer.Dial(wsURL, headers)
	if err != nil {
		return fmt.Errorf("WebSocket connection failed: %v", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	log.Println("WebSocket connection established successfully")
	go c.startPingLoop()
	return nil
}

func (c *MarketDataClient) Subscribe(instrumentKeys []string, mode string) error {
	if !c.connected {
		return fmt.Errorf("not connected to WebSocket")
	}

	if len(instrumentKeys) == 0 {
		return fmt.Errorf("no instrument keys provided")
	}

	// Validate mode
	validModes := map[string]bool{
		"ltpc":          true,
		"option_greeks": true,
		"full":          true,
	}
	if !validModes[mode] {
		return fmt.Errorf("invalid mode: %s", mode)
	}

	subscription := map[string]interface{}{
		"guid":   generateGUID(),
		"method": "sub",
		"data": map[string]interface{}{
			"mode":           mode,
			"instrumentKeys": instrumentKeys,
		},
	}

	payload, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %v", err)
	}

	log.Printf("Sending subscription request: %s", string(payload))

	c.mu.Lock()
	defer c.mu.Unlock()

	// Add write timeout
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	c.conn.SetWriteDeadline(time.Time{}) // Clear deadline

	if err != nil {
		return fmt.Errorf("failed to send subscription: %v", err)
	}

	return nil
}
func (c *MarketDataClient) reconnect() {
	c.Disconnect()
	time.Sleep(2 * time.Second)

	for retry := 0; retry < 3; retry++ {
		if err := c.Connect(); err == nil {
			time.Sleep(2 * time.Second)
			if err := c.Subscribe(instrumentKeys, "ltpc"); err != nil {
				log.Printf("Failed to subscribe after reconnect: %v", err)
				continue
			}
			log.Println("Successfully resubscribed after reconnect")
			// Restart the reading loop
			go c.StartReading()
			return
		}
		time.Sleep(time.Duration(retry+1) * 5 * time.Second)
	}
	log.Println("Max reconnection attempts reached")
}

func (c *MarketDataClient) StartReading() {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered in StartReading:", r)
			c.reconnect()
		}
	}()

	if !c.connected {
		log.Println("Not connected, cannot start reading")
		return
	}

	log.Println("Starting to read messages...")

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Stopping message reader")
			return
		default:
			c.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
			messageType, message, err := c.conn.ReadMessage()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Println("Read timeout - reconnecting...")
					c.reconnect()
					return
				}
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					log.Println("WebSocket closed normally")
					return
				}
				log.Printf("Error reading message: %v - reconnecting...", err)
				c.reconnect()
				return
			}

			switch messageType {
			case websocket.BinaryMessage:
				c.handleBinaryMessage(message)
			case websocket.PingMessage:
				log.Println("Received ping message")
				c.conn.WriteMessage(websocket.PongMessage, nil)
			case websocket.PongMessage:
				log.Println("Received pong message")
			case websocket.CloseMessage:
				log.Println("Received close message")
				c.Disconnect()
				return
			default:
				log.Printf("Received unexpected message type: %d", messageType)
			}
		}
	}
}

func (c *MarketDataClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *MarketDataClient) handleBinaryMessage(message []byte) {
	var feedResponse marketdatafeedpb.FeedResponse

	if err := proto.Unmarshal(message, &feedResponse); err != nil {
		log.Printf("Failed to decode Protobuf: %v", err)
		return
	}

	switch feedResponse.GetType() {
	case marketdatafeedpb.Type_market_info:
		c.handleMarketInfo(&feedResponse)
	case marketdatafeedpb.Type_initial_feed, marketdatafeedpb.Type_live_feed:
		c.handleMarketData(&feedResponse)
	default:
		log.Printf("Received unknown message type: %v", feedResponse.GetType())
	}
}

func (c *MarketDataClient) handleMarketInfo(feedResponse *marketdatafeedpb.FeedResponse) {
	if marketInfo := feedResponse.GetMarketInfo(); marketInfo != nil {
		log.Println("------ Market Status ------")
		for segment, status := range marketInfo.GetSegmentStatus() {
			log.Printf("%s: %s\n", segment, status.String())
		}
		log.Println("--------------------------")
	}
}

func buyten(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := candles[ins].Low

	payload := fmt.Sprintf(`{
		"quantity": "%d",
		"product": "I",
		"validity": "DAY",
		"price": 0,
		"tag": "string",
		"instrument_token": "%s",
		"order_type": "MARKET",
		"transaction_type": "BUY",
		"disclosed_quantity": 0,
		"trigger_price": 0,
		"is_amo": false
		}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order10[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "BUY",
			QUANTITY: quant,
			SLGAP:    .0035 * ltp,
		}

		placed.Append(ins)
		onceplaced.Append(ins)
		log.Println(order10[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func sellten(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := ltp * 1.0035
	payload := fmt.Sprintf(`{
		"quantity": "%d",
		"product": "I",
		"validity": "DAY",
		"price": 0,
		"tag": "string",
		"instrument_token": "%s",
		"order_type": "MARKET",
		"transaction_type": "SELL",
		"disclosed_quantity": 0,
		"trigger_price": 0,
		"is_amo": false
		}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order10[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "SELL",
			QUANTITY: quant,
			SLGAP:    .0035 * ltp,
		}

		placed.Append(ins)
		log.Println(order10[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func stoplossten(ltp float64, ins string) {
	if order10[ins].OTYPE == "BUY" {
		if ltp >= (order10[ins].OPRICE + order10[ins].SLGAP) {

			temp := order10[ins]
			temp.USL += temp.SLGAP
			temp.OPRICE += temp.SLGAP
			order10[ins] = temp
			log.Println(order10[ins])

		}
		if ltp <= order10[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "SELL",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order10[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				log.Println(order10[ins], ltp, ins)

			}

		}
	} else {
		if ltp <= (order10[ins].OPRICE - order10[ins].SLGAP) {

			temp := order10[ins] // Get a copy of the struct
			temp.USL -= temp.SLGAP
			temp.OPRICE -= temp.SLGAP
			order10[ins] = temp
			log.Println(order10[ins])

		}
		if ltp >= order10[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "BUY",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order10[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				log.Println(order10[ins], ltp, ins)

			}
		}
	}
}

func (c *MarketDataClient) handleMarketData(feedResponse *marketdatafeedpb.FeedResponse) {
	for instrument, feed := range feedResponse.GetFeeds() {
		switch feed.GetRequestMode() {
		case marketdatafeedpb.RequestMode_ltpc:
			if ltpc := feed.GetLtpc(); ltpc != nil {
				// log.Printf("\n------ LTPC Data for %s ------\n", instrument)
				log.Printf("Last Traded Price: %.2f\n for %s", ltpc.GetLtp(), instrument)
				ins := instrument
				ltp := ltpc.GetLtp()

				if instrumentData[ins].SL < ltp && ltp < instrumentData[ins].UL {
					closetosma.Append(ins)

				}
				log.Println("Appended")

				ts := time.Now()
				intervalStart := getIntervalStart(ts)
				candle, exists := candleData[ins]
				if !exists || !candle.StartTime.Equal(intervalStart) {
					// Finalize old candle (optional: print or save it)
					if exists {
						if candle.Close < candle.Open && closetosma.Contains(ins) {
							candles[ins] = &Candle{
								Open:      candle.Open,
								High:      candle.High,
								Low:       candle.Low,
								Close:     candle.Close,
								StartTime: candle.StartTime, // time.Time is a struct and is copied by value
							}
							bullish.Append(ins)
							log.Printf("Finalized Candle [%s]: %+v\n", ins, *candle)
						}

					}

					// Start new candle
					candleData[ins] = &Candle{
						Open:      ltp,
						High:      ltp,
						Low:       ltp,
						Close:     ltp,
						StartTime: intervalStart,
					}
				} else {
					// Update ongoing candle
					candle.Close = ltp
					if ltp > candle.High {
						candle.High = ltp
					}
					if ltp < candle.Low {
						candle.Low = ltp
					}
				}

				log.Println("Candling")

				if !bullish.Contains(ins) {
					stockData := struct {
						Instrument string
						Price      float64
					}{
						Instrument: ins, // Simulated instrument key
						Price:      ltp, // Simulated stock price
					}
					select {
					case stockChannel1 <- stockData: // Non-blocking send
					default:
						// Drop old data if buffer is full to always keep recent stock prices
						<-stockChannel1
						stockChannel1 <- stockData
					}
				}
				log.Println("data sent")

			}
		}
	}
}

func processStockData1(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for data := range stockChannel1 {

		ins := data.Instrument
		ltp := data.Price
		log.Println(ins, ltp)

		if !onceplaced.Contains(ins) {
			if candle, exists := candles[ins]; exists && candle != nil {
				if ltp >= candle.High {
					buyten(ltp, ins)
				}
			} else {
				log.Printf("Warning: No candle data for %s", ins)
			}
		}

		if placed.Contains(ins) {
			stoplossten(ltp, ins)

		}

	}
}

func (c *MarketDataClient) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || !c.connected { // Add this check
		return
	}
	if c.connected {
		log.Println("Disconnecting from WebSocket...")
		err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Printf("Error sending close message: %v", err)
		}

		err = c.conn.Close()
		if err != nil {
			log.Printf("Error closing WebSocket: %v", err)
		}

		c.connected = false
		c.cancel()
		log.Println("Disconnected successfully")
	}
}

func getWebSocketURL() (string, error) {
	req, err := http.NewRequest(http.MethodGet, authURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "*/*")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			AuthorizedRedirectUri string `json:"authorizedRedirectUri"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %v", err)
	}

	if result.Status != "success" {
		return "", fmt.Errorf("api returned non-success status: %s", result.Status)
	}

	return result.Data.AuthorizedRedirectUri, nil
}

func generateGUID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (s *SafeSlice) Values() []string {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return append([]string(nil), s.items...) // return a copy for safety
}

func main() {
	if accessToken == "" {
		log.Println("UPSTOX_ACCESS_TOKEN environment variable not set")
		return
	}

	fileContent, err := os.ReadFile("sma_trend_output.json")
	if err != nil {
		fmt.Println("Failed to read file:", err)
		return
	}

	// Declare a map to hold the JSON data

	// Unmarshal the JSON into the map
	if err := json.Unmarshal(fileContent, &instrumentData); err != nil {
		fmt.Println("Failed to parse JSON:", err)
		return
	}

	// Use the data
	for key := range instrumentData {
		insKeys.Append(key) // Append values
	}

	instrumentKeys = insKeys.Values()

	// Set up signal handling for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	client := NewMarketDataClient()

	// Connect to WebSocket
	connectAndSubscribe := func() error {
		if err := client.Connect(); err != nil {
			return fmt.Errorf("failed to connect: %v", err)
		}

		if err := client.Subscribe(instrumentKeys, "ltpc"); err != nil {
			client.Disconnect()
			return fmt.Errorf("failed to subscribe: %v", err)
		}
		return nil
	}
	if err := connectAndSubscribe(); err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect()

	runtime.GOMAXPROCS(runtime.NumCPU()) // Enable true parallelism

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() / 2

	// Start workers for both channels
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go processStockData1(i, &wg)
	}

	// Start WebSocket processing
	go client.StartReading()

	// Add periodic status logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf(
					"Status - Goroutines: %d, Channel1: %d, Channel2: %d, placed: %d, FifteenStock: %d",
					runtime.NumGoroutine(),
					len(stockChannel1),
					len(stockChannel2),
					len(bullish.items),
					len(placed.items),
				)
			case <-interrupt:
				return
			}
		}
	}()

	// Wait for interrupt
	<-interrupt
	log.Println("Shutting down...")
}
