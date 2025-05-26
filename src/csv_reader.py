import csv

# Укажите имя вашего файла
filename = '14411311_202505092007363998.csv'

# Открываем файл и читаем его как CSV
try:
    with open(filename, newline='', encoding='utf-8') as csvfile:
        # Создаем объект reader
        reader = csv.reader(csvfile)

        # Читаем заголовок (если он есть)
        header = next(reader)
        print("Заголовки столбцов:")
        print(header)

        # Читаем и выводим первые 5 строк данных
        print("\nПервые несколько строк с данными:")
        row_count = 0
        for row in reader:
            print(row)
            row_count += 1
            if row_count >= 5:
                break

except FileNotFoundError:
    print(f"Файл '{filename}' не найден. Проверьте путь.")
except Exception as e:
    print(f"Произошла ошибка: {e}")