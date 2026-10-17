object queueLock = new();
object fileLock = new();
SemaphoreSlim semaphore = new(2);
string filePath = "results.txt";
Queue<Order> orderQueue = new Queue<Order>();

File.WriteAllText(filePath, "=== ORDER PROCESSING RESULTS ===\n");

Random random = new Random();
for (int i = 1; i <= 100; i++)
{
    orderQueue.Enqueue(new Order 
    { 
        Id = i, 
        Price = random.Next(100, 1001),
        IsProcessed = false 
    });
}

Console.WriteLine("=== STARTING ORDER PROCESSING SYSTEM ===");

List<Thread> threads = new List<Thread>();
for (int i = 1; i <= 5; i++)
{
    int threadId = i;
    Thread thread = new Thread(() => ProcessOrders(threadId));
    threads.Add(thread);
    thread.Start();
}

foreach (var thread in threads)
{
    thread.Join();
}

State._isRunning = false;

Console.WriteLine("\n=== ALL ORDERS PROCESSED SUCCESSFULLY ===");
Console.WriteLine($"Total processed orders: {Stats.ProcessedCount}");
Console.WriteLine($"Total price:            {Stats.TotalPrice} AZN");
Console.WriteLine($"System _isRunning state: {State._isRunning}");
Console.WriteLine($"Results saved to file:  {filePath}");

Console.ReadLine();

void ProcessOrders(int threadId)
{
    while (State._isRunning)
    {
        Order? order = null;

        bool lockTaken = false;

        try
        {
            Monitor.Enter(queueLock, ref lockTaken);

            if (orderQueue.Count > 0)
            {
                order = orderQueue.Dequeue();
            }
        }
        finally
        {
            if (lockTaken)
            {
                Monitor.Exit(queueLock);
            }
        }

        if (order == null)
        {
            break;
        }

        semaphore.Wait();
        try
        {
            Thread.Sleep(Random.Shared.Next(10, 50));
            order.IsProcessed = true;

            Interlocked.Increment(ref Stats.ProcessedCount);
            Interlocked.Add(ref Stats.TotalPrice, order.Price);

            lock (fileLock)
            {
                File.AppendAllText(filePath, $"[Thread #{threadId}] Processed Order #{order.Id} | Price: {order.Price} AZN\n");
            }
        }
        finally
        {
            semaphore.Release();
        }
    }
}
