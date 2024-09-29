import string
import requests
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import logging

# Налаштування логування
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для завантаження тексту з URL
def get_text(url: str) -> str:
    """
    Завантажує текст за заданою URL-адресою.
    
    :param url: URL для завантаження тексту.
    :return: Текст у вигляді рядка або None у разі помилки.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        logging.error(f"Помилка при завантаженні тексту: {e}")
        return None

# Функція для видалення знаків пунктуації
def remove_punctuation(text: str) -> str:
    """
    Видаляє всі знаки пунктуації з тексту.
    
    :param text: Вхідний текст.
    :return: Текст без пунктуації.
    """
    return text.translate(str.maketrans("", "", string.punctuation))

# Map функція
def map_function(word: str) -> tuple[str, int]:
    """
    Повертає кортеж зі словом і числом 1 для мапінгу.
    
    :param word: Вхідне слово.
    :return: Кортеж (слово, 1).
    """
    return word, 1

# Shuffle функція
def shuffle_function(mapped_values: list[tuple[str, int]]) -> dict[str, list[int]]:
    """
    Перегруповує результати мапінгу, збираючи однакові слова разом.
    
    :param mapped_values: Список кортежів (слово, 1).
    :return: Перегруповані значення (слово, [1, 1, ...]).
    """
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

# Reduce функція
def reduce_function(key_values: tuple[str, list[int]]) -> tuple[str, int]:
    """
    Підраховує загальну кількість кожного слова.
    
    :param key_values: Кортеж (слово, список [1, 1, ...]).
    :return: Кортеж (слово, сума кількості).
    """
    key, values = key_values
    return key, sum(values)

# MapReduce функція
def map_reduce(text: str, search_words=None) -> dict[str, int]:
    """
    Виконує аналіз частоти слів у тексті за допомогою парадигми MapReduce.
    
    :param text: Вхідний текст для обробки.
    :param search_words: враховувати тільки ці слова
    :return: Словник частот слів.
    """
    
    text = remove_punctuation(text)
    words = text.split()

    # Якщо задано список слів для пошуку, враховувати тільки ці слова
    if search_words:
        words = [word for word in words if word in search_words]

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

    

# Функція для візуалізації топ-слів
def visualize_top_words(words: dict[str, int], top_n: int ) -> None:
    """
    Виводить діаграму частоти топ N слів.
    
    :param word_freq: Словник з частотами слів.
    :param top_n: Кількість слів для відображення.
    """
    
    top_words = Counter(words).most_common(top_n)

    msg = f"Топ {top_n} слів за частоою використаення"
    print(msg)
    for word, freq in top_words:
        print(f"{word}: {freq}")

    words, counts = zip(*top_words)

    plt.figure(figsize=(10, 8))
    plt.barh(words, counts, color="skyblue")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(msg)
    plt.gca().invert_yaxis()
    plt.show()
    



if __name__ == '__main__':
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    try:
        text = get_text(url)
        if text:
            # Виконання MapReduce на вхідному тексті
            result = map_reduce(text)

            # Виведення та візуалізація топ слів
            visualize_top_words(result, top_n=15)
        else:
            raise ValueError("Текст не був завантажений.")
    except Exception as e:
        logging.error(f"Помилка в головному блоці: {e}")
        