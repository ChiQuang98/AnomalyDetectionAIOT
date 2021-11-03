package models

import (
	"encoding/json"
	"github.com/gogo/protobuf/proto"
)
//type AnomalyChannel struct {
//	ID string `json:"id"`
//	ChannelID string `json:"channel_id"`
//	Option int `json:"option"`
//	Type int `json:"type"`
//}
type AnomalyChannel struct {
	ID string `json:"id"`
	ChannelID []string `json:"channel_id"`
	Option int `json:"option"`
	Type int `json:"type"`
}
type TokenResponse struct {
	Token string `json:"token"`
}
type Account struct {
	Email string `json:"email"`
	Password string `json:"password"`
}
type Channel struct {
	Channel_id string `json:"channel_id"`
	Channel_name string `json:"channel_name"`
	Thing_id string `json:"thing_id"`
	Thing_key string `json:"thing_key"`
}
type JSONSenML struct {
	Valueksql json.RawMessage `json:"valueksql"`
}
type ResponseChannel struct {
	Total int `json:"total"`
	Offset int `json:"offset"`
	Limit int `json:"limit"`
	Order string `json:"order"`
	Direction string `json:"direction"`
	Data []Channel `json:"data"`

}
type MessageNats struct {
	IdChannel string `json:"id_channel,omitempty"`
	MessageData []byte `json:"message_data,omitempty"`
}
type Message struct {
	Channel              string   `protobuf:"bytes,1,opt,name=channel,proto3" json:"channel,omitempty"`
	Subtopic             string   `protobuf:"bytes,2,opt,name=subtopic,proto3" json:"subtopic,omitempty"`
	Publisher            string   `protobuf:"bytes,3,opt,name=publisher,proto3" json:"publisher,omitempty"`
	Protocol             string   `protobuf:"bytes,4,opt,name=protocol,proto3" json:"protocol,omitempty"`
	Payload              []byte   `protobuf:"bytes,5,opt,name=payload,proto3" json:"payload,omitempty"`
	Created              int64    `protobuf:"varint,6,opt,name=created,proto3" json:"created,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
func (m *Message) Reset()         { *m = Message{} }
func (m *Message) String() string { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()    {}







