using Confluent.Kafka;
using System;
using System.Threading;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "consumer-group-2",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

string topic = "topicApp2";

// Intenta suscribirse al topic con reintentos
while (true)
{
    try
    {
        consumer.Subscribe(topic);
        Console.WriteLine($"📥 [Consumer 2] Suscrito al topic: {topic}");
        break; // Sale del bucle si la suscripción es exitosa
    }
    catch (ConsumeException ex)
    {
        if (ex.Error.Reason.Contains("Unknown topic or partition"))
        {
            Console.WriteLine($"⚠️ El topic '{topic}' no está disponible. Esperando...");
            Thread.Sleep(5000); // Espera 5 segundos antes de reintentar
        }
        else
        {
            Console.WriteLine($"❌ Error al suscribirse al topic: {ex.Error.Reason}");
            return;
        }
    }
}

Console.WriteLine("📥 [Consumer 2] Esperando mensajes...");

while (true)
{
    try
    {
        var consumeResult = consumer.Consume();
        Console.WriteLine($"📥 [Consumer 2] Mensaje recibido: {consumeResult.Value}");
    }
    catch (ConsumeException ex)
    {
        Console.WriteLine($"❌ Error al consumir mensaje: {ex.Error.Reason}");
    }
}
