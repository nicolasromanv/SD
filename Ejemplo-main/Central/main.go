package main

import (
	"fmt"
	"log"
	"context"
	"time"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	pb "github.com/Kendovvul/Ejemplo/Proto"
)

func LabgRPCSubroutine(lab_name string, host_lab string, ch chan bool) {
	connS, err := grpc.Dial(host_lab, grpc.WithInsecure()) //crea la conexion sincrona con el laboratorio

	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	serviceCliente := pb.NewMessageServiceClient(connS)
	
	for {
		//envia el mensaje al laboratorio
		res, err := serviceCliente.Intercambio(context.Background(), 
			&pb.Message{
				Body: "Equipo listo?",
			})
	
		if err != nil {
			panic("No se puede crear el mensaje " + err.Error())
		}

		fmt.Println(res.Body) //respuesta del laboratorio
		time.Sleep(1 * time.Second) //espera de 5 segundos
	}

	connS.Close()
	ch <- true
}

func main () {
	
	qName := "Emergencias" //Nombre de la cola
	hostQ := "localhost"  //Host de RabbitMQ 172.17.0.1
	hostS := "localhost" //Host de un Laboratorio
	connQ, err := amqp.Dial("amqp://guest:guest@"+hostQ+":5672") //Conexion con RabbitMQ

	escuadrones_disponibles := 2 

	ch_disp := [2]bool{true, true}
	ch1 := make(chan bool)
	ch2 := make(chan bool)
	defer close(ch1)
	defer close(ch2)

	if err != nil {log.Fatal(err)}
	defer connQ.Close()

	ch, err := connQ.Channel()
	if err != nil{log.Fatal(err)}
	defer ch.Close()

	q, err := ch.QueueDeclare(qName, false, false, false, false, nil) //Se crea la cola en RabbitMQ
	if err != nil {log.Fatal(err)}

	fmt.Println(q)

	fmt.Println("Esperando Emergencias")
	chDelivery, err := ch.Consume(qName, "", true, false, false, false, nil) //obtiene la cola de RabbitMQ
	if err != nil {
		log.Fatal(err)
	}
	
	for delivery := range chDelivery {
		select {
			case <- ch1:
				ch_disp[0] = true
				escuadrones_disponibles += 1
			case <- ch2:
				ch_disp[1] = true
				escuadrones_disponibles += 1
			default:
		}

		fmt.Println("[Central] Pedido de ayuda del Laboratorio " + string(delivery.Body) + " recibido.") //obtiene el primer mensaje de la cola
		var port string

		switch string(delivery.Body) {
			case "Laboratiorio Kampala - Uganda":
				port = hostS + ":50051"
			case "Laboratiorio Pohang - Korea":
				port = hostS + ":50052"
			case "Laboratiorio Pripiat - Rusia":
				port = ":50053"
			case "Laboratiorio Renca (la lleva) - Chile":
				port = hostS + ":50054"
		}	

		if escuadrones_disponibles == 0 {
			select {
				case <- ch1:
					ch_disp[0] = true
					escuadrones_disponibles += 1
				case <- ch2:
					ch_disp[1] = true
					escuadrones_disponibles += 1
			}
		}

		escuadrones_disponibles -= 1

		if ch_disp[0] {
			go LabgRPCSubroutine(string(delivery.Body), port, ch1)	
			ch_disp[0] = false	
		} else {
			go LabgRPCSubroutine(string(delivery.Body), port, ch2)	
			ch_disp[1] = false	
		}
				
	}

}