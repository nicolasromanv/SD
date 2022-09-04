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

func main () {
	qName := "Emergencias" //Nombre de la cola
	hostQ := "localhost"  //Host de RabbitMQ 172.17.0.1
	hostS := "localhost" //Host de un Laboratorio
	connQ, err := amqp.Dial("amqp://guest:guest@"+hostQ+":5672") //Conexion con RabbitMQ

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
		port := ":50051"  //puerto de la conexion con el laboratorio
		fmt.Println("Pedido de ayuda de " + string(delivery.Body)) //obtiene el primer mensaje de la cola
		connS, err := grpc.Dial(hostS + port, grpc.WithInsecure()) //crea la conexion sincrona con el laboratorio

		if err != nil {
			panic("No se pudo conectar con el servidor" + err.Error())
		}
	
		defer connS.Close()
	
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
			time.Sleep(5 * time.Second) //espera de 5 segundos
		}
	}

}