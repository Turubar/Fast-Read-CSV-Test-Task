using System.Diagnostics;
using System.Globalization;

namespace MyTestApp
{
    internal class Program
    {
        // Путь к .csv файлу
        static string path = "";

        // Переменные для управления состоянием приложения
        static bool pause = false;
        static bool finish = false;
        static bool statistics = false;

        // Таймер
        static Stopwatch timer = new Stopwatch();

        // Кол-во строк всего/обработано
        static long countLinesTotal = 0;
        static long countLinesProcessed = 0;

        // Словари для хранения названий (Key) и кол-ва покупок (Value)
        static decimal amount = 0;
        static Dictionary<string, int> products = new Dictionary<string, int>();
        static Dictionary<string, int> categories = new Dictionary<string, int>();
        static Dictionary<string, int> brands = new Dictionary<string, int>();

        // Объекты-локеры для обеспечения потокобезопасноти
        static object lockerProcessing = new object();
        static object lockerStatistics = new object();
        static object lockerPause = new object();
        static object lockerFinish = new object();

        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");

            if (args.Length != 1)
            {
                Console.WriteLine("Путь к файлу не указан!");
                Console.WriteLine("Конец работы программы");
                Environment.Exit(0);
            }

            path = args[0];

            FileInfo mainFile = new FileInfo(path);
            FileInfo? dataFile = null;

            // Проверяем файлы и подсчитываем кол-во строк во входном файле
            if (mainFile.Exists)
            {
                if (mainFile.Extension != ".csv")
                {
                    Console.WriteLine("Расширение файла должно быть [.csv]");
                    Console.WriteLine("Конец работы программы");
                    Environment.Exit(0);
                }

                string pathToDataFile = mainFile.FullName.Replace(".csv", "") + "_data.txt";
                dataFile = new FileInfo(pathToDataFile);

                if (dataFile.Exists)
                {
                    bool result = false;
                    using (var reader = new StreamReader(dataFile.FullName))
                    {
                        string? line =  reader.ReadLine();
                        result = long.TryParse(line, out countLinesTotal);
                    }

                    if (!result || countLinesTotal <= 0)
                    {
                        Console.WriteLine("Подсчитываем кол-во строк в файле...");
                        timer.Start();
                        countLinesTotal = ServiceClass.GetRowCount(mainFile.FullName);
                        timer.Stop();

                        Console.WriteLine($"На подсчет было затрачено: {timer.ElapsedMilliseconds / 1000} с.");
                        Thread.Sleep(3000);

                        File.WriteAllText(dataFile.FullName, countLinesTotal.ToString());
                    }
                }
                else
                {
                    Console.WriteLine("Подсчитываем кол-во строк в файле...");
                    timer.Start();
                    countLinesTotal = ServiceClass.GetRowCount(mainFile.FullName);
                    timer.Stop();

                    Console.WriteLine($"На подсчет было затрачено: {timer.ElapsedMilliseconds / 1000} с.");
                    Thread.Sleep(3000);

                    File.WriteAllText(dataFile.FullName, countLinesTotal.ToString());
                }
            }
            else
            {
                Console.WriteLine("Файл по пути: [" + path + "] не найден!");
                Console.WriteLine("Конец работы программы");
                Environment.Exit(0);
            }

            Console.Clear();
            Console.WriteLine($"Входной файл: [{mainFile.FullName}], Буферный файл: [{dataFile.FullName}]");
            Console.WriteLine();

            // Создаем и запускаем поток, который будет читать и обрабатывать файл
            Thread readerThread = new Thread(new ThreadStart(ReadFile));
            timer.Restart();
            timer.Start();
            readerThread.Start();

            // Создаем и запускаем поток, который выводить прогресс обработки
            Thread statisticsThread = new Thread(new ThreadStart(GetStatistics));
            statisticsThread.Start();

            // Ждем нажатия Пробела (Spacebar), если нужно остановить/запустить работу приложения
            while (!finish)
            {
                if (Console.KeyAvailable)
                {
                    if (Console.ReadKey(true).Key == ConsoleKey.Spacebar)
                    {
                        lock (lockerPause)
                        {
                            lock (lockerStatistics)
                            {
                                if (pause)
                                {
                                    pause = false;
                                    statistics = true;
                                    timer.Start();
                                }
                                else
                                {
                                    pause = true;
                                    statistics = true;
                                    timer.Stop();
                                }
                            }
                        }
                    }
                }
            }
        }

        static void ReadFile()
        {
            foreach (var linesPart in ServiceClass.ReadAllLines(path, 300_000))
            {
                foreach (var line in linesPart)
                {
                    string[] columns = line.Split(",");

                    lock (lockerProcessing)
                    {
                        if (countLinesProcessed >= countLinesTotal) break;

                        if (columns[1] == "purchase")
                        {
                            if (products.ContainsKey(columns[2])) products[columns[2]] += 1;
                            else products.Add(columns[2], 1);

                            if (categories.ContainsKey(columns[3])) categories[columns[3]] += 1;
                            else categories.Add(columns[3], 1);

                            if (columns[5] != "")
                            {
                                if (brands.ContainsKey(columns[5])) brands[columns[5]] += 1;
                                else brands.Add(columns[5], 1);
                            }

                            amount += decimal.Parse(columns[6], NumberStyles.AllowDecimalPoint);
                        }

                        countLinesProcessed++;
                    }

                    if (pause)
                    {
                        while (pause)
                        {
                            lock (lockerPause)
                            {
                                if (!pause) break;
                            }
                        }
                    }
                }

                lock (lockerStatistics)
                {
                    statistics = true;
                }
            }

            lock (lockerProcessing)
            {
                if (countLinesTotal > countLinesProcessed) countLinesTotal = countLinesProcessed;
            }

            lock (lockerStatistics)
            {
                statistics = true;
            }
        }

        static void GetStatistics()
        {
            // Выводим прогресс обработки в режиме реального времени
            while (!finish)
            {
                lock (lockerStatistics)
                {
                    if (statistics)
                    {
                        lock (lockerProcessing)
                        {
                            lock (lockerPause)
                            {
                                if (countLinesProcessed <= 0 || countLinesTotal <= 0) continue;
                                if (products.Count <= 0 || categories.Count <= 0 || brands.Count <= 0) continue;

                                Console.SetCursorPosition(0, 2);
                                Console.Write("Обработано: ");
                                Console.SetCursorPosition(12, 2);
                                Console.Write("                                                                             ");
                                Console.SetCursorPosition(12, 2);
                                float percent = ((float)countLinesProcessed / countLinesTotal) * 100;
                                Console.Write(percent.ToString("F2") + $"% ({countLinesTotal} / {countLinesProcessed}) (Время: {timer.Elapsed.TotalSeconds.ToString("F2")} с. / Пауза: {pause})");

                                Console.SetCursorPosition(0, 3);
                                Console.Write("Сумма выручки: ");
                                Console.SetCursorPosition(15, 3);
                                Console.Write("                                                                             ");
                                Console.SetCursorPosition(15, 3);
                                Console.Write(amount.ToString("C2"));

                                Console.SetCursorPosition(0, 4);
                                Console.Write("Самый популярный товар: ");
                                Console.SetCursorPosition(24, 4);
                                Console.Write("                                                                             ");
                                Console.SetCursorPosition(24, 4);
                                var product = products.MaxBy(x => x.Value);
                                Console.Write(product.Key + $" ({product.Value})");

                                Console.SetCursorPosition(0, 5);
                                Console.Write("Самая популярная категория: ");
                                Console.SetCursorPosition(28, 5);
                                Console.Write("                                                                             ");
                                Console.SetCursorPosition(28, 5);
                                var category = categories.MaxBy(x => x.Value);
                                Console.WriteLine(category.Key + $" ({category.Value})");

                                Console.SetCursorPosition(0, 6);
                                Console.Write("Самый популярный бренд: ");
                                Console.SetCursorPosition(24, 6);
                                Console.Write("                                                                             ");
                                Console.SetCursorPosition(24, 6);
                                var brand = brands.MaxBy(x => x.Value);
                                Console.Write(brand.Key + $" ({brand.Value})");
                                Console.WriteLine();

                                if (countLinesProcessed == countLinesTotal)
                                {
                                    lock (lockerFinish)
                                    {
                                        finish = true;
                                    }
                                }

                                statistics = false;
                            }
                        }
                    }
                }
                Thread.Sleep(50);
            }

            // После завершения обработки выводим дополнительную информацию
            Console.WriteLine();
            Console.WriteLine("Суммарное кол-во покупок: " + products.Values.Sum());

            Console.WriteLine();

            Console.WriteLine("Топ-5 товаров: ");
            var maxProducts = products.OrderByDescending(x => x.Value).Take(5);
            foreach (var product in maxProducts)
            {
                Console.WriteLine($"{product.Key} ({product.Value})");
            }

            Console.WriteLine();

            Console.WriteLine("Топ-5 категорий: ");
            var maxCategories = categories.OrderByDescending(x => x.Value).Take(5);
            foreach (var category in maxCategories)
            {
                Console.WriteLine($"{category.Key} ({category.Value})");
            }

            Console.WriteLine();

            Console.WriteLine("Топ-5 брендов: ");
            var maxBrands = brands.OrderByDescending(x => x.Value).Take(5);
            foreach (var brand in maxBrands)
            {
                Console.WriteLine($"{brand.Key} ({brand.Value})");
            }
        }
    }
}