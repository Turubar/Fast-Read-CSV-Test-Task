namespace MyTestApp
{
    internal static class ServiceClass
    {
        // Метод, который возвращает кол-во строк в файле
        public static long GetRowCount(string pathToFile)
        {
            long count = 0;
            using (var reader = new StreamReader(pathToFile))
            {
                reader.ReadLine();
                while (!reader.EndOfStream)
                {
                    reader.ReadLine();
                    count++;
                }
            }

            return count;
        }

        // Метод, который считывает файл и возвращает списки строк заданного размера
        public static IEnumerable<List<string>> ReadAllLines(string pathToFile, long listSize)
        {
            List<string> lineList = new List<string>();

            using (var reader = new StreamReader(pathToFile))
            {
                reader.ReadLine();
                long count = 0;

                while (reader.EndOfStream != true)
                {
                    lineList.Add(reader.ReadLine());
                    count++;

                    if (count >= listSize)
                    {
                        count = 0;
                        yield return lineList;
                        lineList.Clear();
                    }
                }

                if (count > 0 && count <= listSize)
                {
                    yield return lineList;
                }
            }
        }
    }
}
