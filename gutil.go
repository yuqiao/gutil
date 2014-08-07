package main

import (
	"flag"
	"fmt"
	"github.com/gmallard/stompngo"
	"github.com/golang/glog"
	"gutil/stomp"
)

func main() {
	flag.Parse()
	s := "mq://wangxin:wangxin@10.232.136.85:61613/topic/wqupdate?id=stomptest&name=test"
	conn, e := stomp.GetStompConn(s)
	if e != nil {
		glog.Fatalln("get stomp failed.", e)
	}

	fmt.Println("conn:", conn)
	fmt.Println("heder:", conn.ConnectHeaders())
	r := conn.Subscribe()

	for i := 1; i <= 10; i++ {
		m := <-r
		fmt.Println("channel read complete ...")
		if m.Error != nil {
			glog.Fatalln(m.Error)
		}
		fmt.Printf("Frame Type: %s\n", m.Message.Command)
		if m.Message.Command != stompngo.MESSAGE {
			fmt.Println("do not handle the message")
			continue
		}
		h := m.Message.Headers
		for j := 0; j < len(h)-1; j += 2 {
			fmt.Printf("Header: %s:%s", h[j], h[j+1])
		}
		fmt.Printf("Payload: %s\n", string(m.Message.Body))
	}

	conn.Unsubscribe()
	conn.Close()
}
