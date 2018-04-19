package eventq

import (
	"sync"
	"fmt"
	"time"
)
//事件接口
type EventI interface {
	ID() int
}

//事件结构体
type Event struct {
	Id int
	EventData EventI 	//事件数据
	HEvent *Event 		//上一个节点
	LEvent *Event		//下一个节点
	JoinTime time.Time 	//加入时间
	TestGetTime time.Time 	//测试字段 统计取数据用时
}


//事件队列结构体
type EventQ struct {
	EQID string 		//队列id
	Events *Event		//事件链表
	Lock   *sync.Mutex 	//队列操作原子锁
	isclose bool 		//停止开关
	TailEvent *Event 	//事件队列尾指针
	EventIndex map[int]*Event 	//事件索引
	Cond *sync.Cond
}

//队列集合类型
type Eqs struct{
	Eqms map[string]*EventQ
	Lock *sync.Mutex
}
//队列集合
var eqs Eqs

//新建队列
func New() (eq *EventQ, err error) {
	eqs.Lock.Lock()
	defer eqs.Lock.Unlock()
	eventqid := fmt.Sprintf("%d",time.Now().UnixNano())
	var ok bool
	if eq , ok = eqs.Eqms[eventqid]; ok {
		err = fmt.Errorf("init eq fail with same id")
		return
	}
	locker := new(sync.Mutex)
	eq = &EventQ{
		EQID: eventqid,
		Events: nil,
		TailEvent : nil,
		EventIndex : map[int]*Event{},
		Lock : new(sync.Mutex),
		Cond : sync.NewCond(locker),
	}
	eqs.Eqms[eventqid] = eq
	return
}
//获取队列
func GetEQ(eventqid string) (*EventQ, error){
	eqs.Lock.Lock()
	defer eqs.Lock.Unlock()
	if eq , ok := eqs.Eqms[eventqid]; ok {
		return eq, nil
	} else {
		return nil, fmt.Errorf("error eventqid")
	}
}

//移除队列
func RmEQ(eventqid string) {
	eqs.Lock.Lock()
	defer eqs.Lock.Unlock()
	if eq, ok := eqs.Eqms[eventqid];ok {
		eq.Close()
		delete(eqs.Eqms, eventqid)
	}
}



//初始化事件队列结构体
func init() {
	eqs = Eqs{
		Eqms: map[string]*EventQ{},
		Lock: new(sync.Mutex),
	}
}

//添加事件
func (eq *EventQ) Add(eventi EventI) (err error) {
	t1 := time.Now()
	if eq.isclose{
		return fmt.Errorf("is close")
	}

	event := &Event{
		Id: eventi.ID(),
		EventData: eventi,
		JoinTime: time.Now(),
	}
	if event.Id == 0{
		return fmt.Errorf("miss eventid")
	}
	eq.Lock.Lock()
	defer eq.Lock.Unlock()
	err = eq.check_and_reset(event)
	if err != nil {
		return
	}
	//初始节点
	if eq.Events == nil {
		eq.Events = event
		eq.TailEvent = event
	} else {
		//非初始节点
		event.HEvent = eq.TailEvent
		eq.TailEvent.LEvent = event
		eq.TailEvent = event
	}
	eq.EventIndex[event.Id] = event
	eq.Cond.Signal()
	fmt.Println("add event",event.Id, "num:",len(eq.EventIndex), time.Now().Sub(t1))
	return nil
}
//事件去重
func (eq *EventQ) check_and_reset(event *Event) (err error) {
	if eq_event, ok := eq.EventIndex[event.Id]; ok {
		//链表中间
		if eq_event.LEvent != nil && eq_event.HEvent != nil  {
			eq_event.HEvent.LEvent = eq_event.LEvent
			return
		} else if eq_event.HEvent == nil {	// 链表第一个
			eq.Events = nil
			eq.TailEvent = nil
			return
		} else if eq_event.LEvent == nil {
			//链表最后一个
			eq_event.HEvent.LEvent = nil
			eq.TailEvent = eq_event.HEvent
			return
		}
	}
	return

}
//事件获取并执行
func (eq *EventQ) Get(f func(EventI) error) ( err error) {
	event_chan := make(chan *Event)
	sin_chan := make(chan bool)
	defer close(event_chan)
	go eq.get_cell(event_chan, sin_chan)
	for {
		select {
		case event := <- event_chan:
			fmt.Println("event in queue time ",time.Now().Sub(event.JoinTime), time.Now().Sub(event.TestGetTime))
			err = f(event.EventData)
			if err != nil {
				eq.Add(event.EventData)
				fmt.Println("consumer re add:",event.Id)
			}
		case <- sin_chan:
			return
		}

	}
	return
}

//事件单元获取
func (eq *EventQ) get_cell(event_chan chan<- *Event, sin_chan chan<- bool) (err error) {
	for {
		if eq.isclose {
			err = fmt.Errorf("is close")
			close(sin_chan)
			return
		}

		eq.Lock.Lock()

		if eq.Events == nil {
			eq.Lock.Unlock()
			eq.Cond.L.Lock()
			fmt.Println("waiting...")
			eq.Cond.Wait() 		//等待通知
			eq.Cond.L.Unlock() 	//收到通知
			continue
		} else {
			event := eq.Events
			event.TestGetTime = time.Now()
			eq.Events = eq.Events.LEvent	//将头节点移除 如果头节点为最后一个节点 将被赋值nil
			delete(eq.EventIndex, event.Id)
			eq.Lock.Unlock()
			event_chan<- event
		}
	}

}

//队列长度
func (eq *EventQ)Len() int {
	return len(eq.EventIndex)
}

//关闭队列
func(eq *EventQ) Close(){
	eq.isclose = true 	//设置开关
	eq.Cond.Broadcast() 	//广播关掉所有goroutine
}
//队列恢复
func(eq *EventQ) ReOpen(){
	eq.isclose = false
}

