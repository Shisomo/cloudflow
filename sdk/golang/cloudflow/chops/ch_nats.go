package chops

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsChOp struct {
	url      string
	chprefix string
	nc       *nats.Conn
	js       nats.JetStreamContext
	st       *nats.StreamInfo
	subs     map[string]*nats.Subscription
	pulls    map[string]*nats.Subscription
	csmr     map[string]*nats.ConsumerInfo
}

func NewNatsChOp(nc_url string, stream_name string) *NatsChOp {
	nc, err := nats.Connect(nc_url)
	cf.Assert(err == nil, "connet(%s) Nats error: %s", nc_url, err)
	js, err := nc.JetStream()
	cf.Assert(err == nil, "create jetstream error: %s", err)
	st, err := js.AddStream(&nats.StreamConfig{
		Name: stream_name,
		//Retention: nats.WorkQueuePolicy,
		//Retention: nats.InterestPolicy,
		Subjects: []string{stream_name + ".>"},
	})

	cf.Assert(err == nil, "create js.stream(%s) fail: %s", stream_name, err)
	ops := NatsChOp{
		url:      nc_url,
		chprefix: stream_name,
		nc:       nc,
		js:       js,
		st:       st,
		subs:     map[string]*nats.Subscription{},
		pulls:    map[string]*nats.Subscription{},
		csmr:     map[string]*nats.ConsumerInfo{},
	}
	return &ops
}

func (nt *NatsChOp) Put(ch_name []string, value string) bool {
	cf.Assert(len(value) > 0, "message is empty(val:%s)", value)
	chs := nt.toSubjects(ch_name)
	for _, ch := range chs {
		_, err := nt.js.Publish(ch, []byte(value))
		cf.Assert(err == nil, "publish message to %s fail:%s", ch, err)
	}
	return true
}

func (nt *NatsChOp) Watch(who string, ch_name []string, fc func(queue_nm string, subj string, data string) bool) []string {
	cnkey := []string{}
	for _, sb := range nt.toSubjects(ch_name) {
		queue_name := cf.AsMd5(sb)
		sb_name := strings.ReplaceAll(sb, ".", "-")
		//nt.addConsumer(sb, queue_name, sb_name)
		sub, err := nt.js.QueueSubscribe(sb, queue_name, func(m *nats.Msg) {
			subject := strings.Replace(m.Subject, nt.chprefix+".", "", 1)
			fc(queue_name, subject, string(m.Data))
			//m.Ack()
		}, nats.AckExplicit(), nats.Durable(sb_name))
		cf.Assert(err == nil, "create QueueSubscribe(subj: %s) fail: %s", sb, err)
		key := "que" + cf.AsMd5(cf.DotS(queue_name, cf.TimestampStr()))
		nt.subs[key] = sub
		cnkey = append(cnkey, key)
	}
	return cnkey
}

func (nt *NatsChOp) Sub(who string, ch_name []string, fc func(sb_name string, subj string, data string) bool) []string {
	cnkey := []string{}
	for _, sb := range nt.toSubjects(ch_name) {
		sb_name := strings.ReplaceAll(sb, ".", "-") + "-" + who
		sub, err := nt.js.Subscribe(sb, func(m *nats.Msg) {
			subject := strings.Replace(m.Subject, nt.chprefix+".", "", 1)
			fc(sb_name, subject, string(m.Data))
			//m.Ack()
		}, nats.AckExplicit(), nats.Durable(sb_name))
		cf.Assert(err == nil, "create QueueSubscribe(subj: %s) fail: %s", sb, err)
		key := "sub" + cf.AsMd5(cf.DotS(sb_name, cf.TimestampStr()))
		nt.subs[key] = sub
		cnkey = append(cnkey, key)
	}
	return cnkey
}

func (nt *NatsChOp) CEmpty(cnkey []string) bool {
	for _, k := range cnkey {
		p, _, e := nt.subs[k].Pending()
		cf.Assert(e == nil, "%s", e)
		if p > 0 {
			return false
		}
	}
	return true
}

/**
func (nt *NatsChOp) addConsumer(sb string, qname string, sb_name string) {
	_, has := nt.csmr[qname]
	if has {
		return
	}
	c, e := nt.js.AddConsumer(nt.chprefix, &nats.ConsumerConfig{
		Durable:        sb_name,
		DeliverSubject: qname,
		DeliverGroup:   qname,
		AckPolicy:      nats.AckExplicitPolicy,
	})
	cf.Assert(e == nil, "add consumer fail: %s", e)
	nt.csmr[qname] = c
}
**/

func (nt *NatsChOp) Get(who string, ch_name []string, timeout time.Duration) []string {
	// TBD
	return nil
}

func (nt *NatsChOp) CStop(cnkey []string) bool {
	for _, key := range cnkey {
		worker, val := nt.subs[key]
		if val {
			continue
		}
		err := worker.Unsubscribe()
		cf.Assert(err == nil, "unsub[%s] fail: %s", worker.Subject, err)
		delete(nt.subs, key)
	}
	return true
}

func (nt *NatsChOp) Close() bool {
	nt.js.PurgeStream(nt.chprefix)
	nt.js.DeleteStream(nt.chprefix)
	nt = nil
	return true
}

func (nt *NatsChOp) toSubjects(ch_name []string) []string {
	return cf.AddPrefix(ch_name, nt.chprefix+".")
}

func askey(ch_name []string) string {
	return cf.AsMd5(strings.Join(ch_name, "."))
}
