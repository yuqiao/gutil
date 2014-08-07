package stomp

import (
	"errors"
	"github.com/gmallard/stompngo"
	"github.com/golang/glog"
	"net"
	"net/url"
	"strings"
)

type StompInfo struct {
	id     string
	host   string
	port   string
	user   string
	passwd string
	topic  string
}

type StompConn struct {
	*StompInfo
	uuid       string
	network    net.Conn
	connection *stompngo.Connection
}

func GetStompInfo(urlStr string) (info *StompInfo, e error) {
	info = new(StompInfo)
	u, e := url.Parse(urlStr)
	if e != nil {
		return
	}

	if u.Scheme != "mq" {
		glog.Error("invalid scheme:" + u.Scheme)
		e = errors.New("invalid scheme:" + u.Scheme)
		return
	}

	h := strings.Split(u.Host, ":")
	info.host = h[0]
	if len(h) == 2 {
		info.port = h[1]
	} else {
		info.port = "61613"
	}

	info.user = u.User.Username()
	info.passwd, _ = u.User.Password()
	info.id = u.Query().Get("id")
	info.topic = u.Path
	return info, nil
}

func GetStompConn(urlStr string) (conn *StompConn, e error) {
	conn = new(StompConn)
	conn.StompInfo, e = GetStompInfo(urlStr)
	if e != nil {
		return nil, e
	}

	conn.network, e = net.Dial("tcp", net.JoinHostPort(conn.host, conn.port))
	if e != nil {
		glog.Error("connect address("+conn.host+":"+conn.port+") failed", e)
		return nil, e
	}
	glog.Info(conn.id + " dail complete.")
	conn.connection, e = stompngo.Connect(conn.network, conn.ConnectHeaders())

	if e != nil {
		glog.Error("stomp connect failed!", e)
		return nil, e
	}

	glog.Info(conn.id + " stomp connect complete ...")
	conn.uuid = stompngo.Uuid()

	return conn, nil
}

func (s StompInfo) String() string {
	return "[StompInfo] id=" + s.id + " user=" + s.user + " topic=" + s.topic
}

func (s StompConn) String() string {
	return "[StompConn] id=" + s.id + " user=" + s.user + " topic=" + s.topic
}

func (s *StompInfo) ConnectHeaders() stompngo.Headers {
	h := stompngo.Headers{}
	h = h.Add("login", s.user)
	h = h.Add("passcode", s.passwd)
	h = h.Add("accept-version", stompngo.SPL_11).Add("host", s.host)
	return h
}

func (s *StompConn) Subscribe() <-chan stompngo.MessageData {
	h := stompngo.Headers{"destination", s.topic, "ack", "auto"}
	h = h.Add("id", s.id)
	r, e := s.connection.Subscribe(h)
	if e != nil {
		glog.Fatalln("subscribe failed", e)
	}
	return r
}

func (s *StompConn) Unsubscribe() {
	h := stompngo.Headers{}

	h = h.Add("id", s.id)
	e := s.connection.Unsubscribe(h)
	if e != nil {
		glog.Fatalln("unsubscribe failed.", e)
	}
	return
}

func (s *StompConn) Close() {
	s.connection.Disconnect(stompngo.Headers{})
	glog.Info(s.id + " stomp disconnect complete")
	s.network.Close()
	glog.Info(s.id + " network close complete")
}
