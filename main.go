package main

import "test/eventq"
import "fmt"
import "time"


type NewEvent struct  {
	eventq.EventI
	Id int
	Data string
}

func (e NewEvent) ID() int{
	return e.Id
}


func main() {


	//消费者1 返回正确
	f1 := func(event eventq.EventI )error{
		newevent := event.(NewEvent)
		fmt.Println("consumer1:right:",newevent.ID(), newevent.Data)
		return nil
	}
	//消费者2 返回错误
	f2 := func(event eventq.EventI )error{
		newevent := event.(NewEvent)
		fmt.Println("consumer2:error:",newevent.ID(), newevent.Data)
		return fmt.Errorf("err")
	}

	//创建队列
	eq ,_ := eventq.New()
	//正常消费
	for i:= 1; i<= 5; i++ {
		go eq.Get(f1)
	}

	//失败消费
	go eq.Get(f2)

	//添加事件
	for i:= 1; i<=1000 ; i++ {
		e := NewEvent{
			Id: i,
			Data: fmt.Sprintf("test%v",i),
		}
		eq.Add(e)
	}

	time.Sleep(5*time.Second) //方便演示
	fmt.Println("eq length:", eq.Len())
	eventq.RmEQ(eq.EQID)  //移除队列

}
