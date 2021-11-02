package utils

import (
	"fmt"
	"net"
)

func ResolveHostIp() (string, error) {
	netInterfaceAddresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, netInterfaceAddress := range netInterfaceAddresses {
		networkIp, ok := netInterfaceAddress.(*net.IPNet)
		if ok && !networkIp.IP.IsLoopback() && networkIp.IP.To4() != nil {
			ip := networkIp.IP.String()
			return ip, nil
		}
	}
	return "", err
}
type Broker struct {
	hosts[] string
	port int
}
func (b Broker) String() string {
	var bootstrap string
	for i:=0;i<len(b.hosts)-1;i++{
		bootstrap += fmt.Sprintf("%v:%v,", b.hosts[i], b.port)
	}
	bootstrap += fmt.Sprintf("%v:%v", b.hosts[len(b.hosts)-1], b.port)
	return bootstrap
}
func SetBroker(brokers []string,port int) Broker {
	b:=Broker{
		hosts: brokers,
		port: port,
	}
	return b
}
