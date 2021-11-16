package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	mail "github.com/xhit/go-simple-mail/v2"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

// Backend holds the data about a server
type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

// SetAlive for this backend
func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

// IsAlive returns true when backend is alive
func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	alive = b.Alive
	b.mux.RUnlock()
	return
}

// ServerPool holds information about reachable backends
type ServerPool struct {
	backends []*Backend
	current  uint64
}

// AddBackend to the server pool
func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

// NextIndex atomically increase the counter and return an index
func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// MarkBackendStatus changes a status of a backend
func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	for _, b := range s.backends {
		if b.URL.String() == backendUrl.String() {
			b.SetAlive(alive)
			break
		}
	}
}

// GetNextPeer returns next active peer to take a connection
func (s *ServerPool) GetNextPeer() *Backend {
	// loop entire backends to find out an Alive backend
	next := s.NextIndex()
	l := len(s.backends) + next // start from next and move a full cycle
	for i := next; i < l; i++ {
		idx := i % len(s.backends)     // take an index by modding
		if s.backends[idx].IsAlive() { // if we have an alive backend, use it and store if its not the original one
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx]
		}
	}
	return nil
}

func sendAlertEmail(message string) {
	smtpServer := mail.NewSMTPClient()
	smtpServer.Host = emailServer
	smtpServer.Port = 25
	client, err := smtpServer.Connect()
	if err != nil {
		log.Fatal(err)
	}

	email := mail.NewMSG()
	email.SetFrom(fmt.Sprintf("%s@%s", loadBalancerName, sendingDomain)).
		SetSubject(fmt.Sprintf("Re:- %s Node down", loadBalancerName)).
		SetPriority(mail.PriorityHigh).
		SetBody(mail.TextHTML, message)

	for _, to := range strings.Split(alertEmailAddresses, ";") {
		email.AddTo(to)
	}

	if err := email.Send(client); err != nil {
		log.Fatal(err)
	}
}

// HealthCheck pings the backends and update the status
func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "up"
		alive := isBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "down"
			sendAlertEmail(fmt.Sprintf("%s %s", b.URL, status))
			logError(fmt.Sprintf("%s %s", b.URL, status), errors.New("node unreachable"))
		} else {
			logInfo(fmt.Sprintf("%s %s", b.URL, status))
		}
	}
}

// GetAttemptsFromContext returns the attempts for request
func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

// GetAttemptsFromContext returns the attempts for request
func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

// lb load balances the incoming request
func lb(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	peer := serverPool.GetNextPeer()
	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

// isAlive checks whether a backend is Alive by establishing a TCP connection
func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("service unreachable, error: ", err)
		return false
	}
	_ = conn.Close()
	return true
}

// healthCheck runs a routine for check status of the backends every 2 mins
func healthCheck() {
	t := time.NewTicker(time.Minute * 2)
	for {
		select {
		case <-t.C:
			logInfo("starting node health checks...")
			serverPool.HealthCheck()
			logInfo("node health checks completed")
		}
	}
}

var (
	serverPool          ServerPool
	serverList          string
	serverPort          int
	logger              *logrus.Logger
	emailServer         string
	loadBalancerName    string
	alertEmailAddresses string
	sendingDomain string
)

func setConfiguration() {
	path, err := os.Getwd()
	if err != nil {
		log.Fatal("unable to determine current working directory: " + err.Error())
	}

	path += "/"

	viper.SetConfigFile("config.yaml")
	viper.AddConfigPath(path)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("unable to read service configuration file: " + err.Error())
	}

	serverList = viper.GetString("backends")
	serverPort = viper.GetInt("server_port")

	logger = logrus.New()
	logger.Out = os.Stdout
	file, err := os.OpenFile("load_balancer_event.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		logger.Out = file
	} else {
		logger.Error("Failed to log to file, using default stderr")
	}

	emailServer = viper.GetString("email_server")
	loadBalancerName = viper.GetString("load_balancer_name")
	alertEmailAddresses = viper.GetString("alert_email_addresses")
	sendingDomain = viper.GetString("sending_domain")
}

func logError(errorMessage string, err error) {
	logger.WithFields(logrus.Fields{
		"msg": errorMessage,
	}).Error(err.Error())
}

func logInfo(message string) {
	logger.WithFields(logrus.Fields{
		"msg": message,
	}).Info(message)
}

func main() {
	setConfiguration()
	if len(serverList) == 0 {
		log.Fatal("Please provide one or more backends to load balance")
	}

	// parse servers
	tokens := strings.Split(serverList, ",")
	for _, tok := range tokens {
		serverUrl, err := url.Parse(tok)
		if err != nil {
			logError("error parsing tokens", err)
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			logError(fmt.Sprintf("%s", serverUrl.Host), e)
			log.Printf("[%s] %s\n", serverUrl.Host, e.Error())

			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			// after 3 retries, mark this backend as down
			serverPool.MarkBackendStatus(serverUrl, false)

			// if the same request routing for few attempts with different backends, increase the count
			attempts := GetAttemptsFromContext(request)

			logInfo(fmt.Sprintf("%s(%s) Attempting retry %d", request.RemoteAddr, request.URL.Path, attempts))
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)

			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			lb(writer, request.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})
		log.Printf("Configured server: %s\n", serverUrl)
	}

	// create http server
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", serverPort),
		Handler: http.HandlerFunc(lb),
	}

	// start health checking
	go healthCheck()

	logInfo(fmt.Sprintf("Load Balancer started at :%d\n", serverPort))
	log.Printf("Load Balancer started at :%d\n", serverPort)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
