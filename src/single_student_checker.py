import pandas as pd
import ollama
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# === НАСТРОЙКИ ===
ETHALON_PATH = "data/ethalons.xlsx"
CSV_PATH = "14411311_202505092007363998.csv"
DISCIPLINE = "Строительное оборудование"
LECTURE_ID = "Lec01"
ID_COLUMN_NAME = "13.**Укажите Ваш ID:**"
LLM_MODEL = "mistral"  # Запусти ollama run mistral

# === 1. Загрузка эталонов ===
ethalons_df = pd.read_excel(ETHALON_PATH)
ethalons = ethalons_df[
    (ethalons_df["Discipline"] == DISCIPLINE) &
    (ethalons_df["Lecture_ID"] == LECTURE_ID)
].sort_values("Question_ID")

ethalon_questions = ethalons["Question"].astype(str).tolist()
ethalon_answers = ethalons["Answer"].astype(str).tolist()
num_questions = len(ethalon_answers)

# === 2. Загрузка CSV ===
csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", engine="python")

# === 3. Определяем начало вопросов ===
question_start_index = None
for i, col in enumerate(csv_df.columns):
    if "Как называется" in col or col.strip().startswith("1."):
        question_start_index = i
        break
if question_start_index is None:
    raise ValueError("❌ Не удалось найти начало вопросов в CSV!")

# === 4. Функция оценки через LLM ===
# === Функция оценки через LLM ===
def grade_with_llm(question, reference_answer, student_answer):
    if not student_answer.strip():
        return 0, "Пустой ответ"

    prompt = f"""
Ты — преподаватель. Проверь, насколько ответ студента совпадает с эталонным по смыслу.

Вопрос: {question}
Эталонный ответ: {reference_answer}
Ответ студента: {student_answer}

Оцени по шкале:
- 0 — не по теме
- 1 — частично верно
- 2 — полностью верно

Ответь только числом: 0, 1 или 2.
"""

    try:
        response = ollama.chat(model=LLM_MODEL, messages=[
            {"role": "user", "content": prompt}
        ])
        reply = response["message"]["content"].strip()
        score = int([s for s in reply.split() if s.isdigit()][0])
        return min(max(score, 0), 2), reply
    except Exception as e:
        return 0, f"Ошибка: {e}"

# === Параллельная проверка вопросов студента ===
def grade_student(student_id, questions, reference_answers, student_answers):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    total_score = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_question = {
            executor.submit(grade_with_llm, q, r, a): (i, q, r, a)
            for i, (q, r, a) in enumerate(zip(questions, reference_answers, student_answers), 1)
        }

        for future in as_completed(future_to_question):
            i, question, correct_ans, student_ans = future_to_question[future]
            score, feedback = future.result()
            total_score += score

            print(f"\nВопрос {i}: {question}")
            print(f"🔹 Эталон:  {correct_ans}")
            print(f"🔸 Ответ:   {student_ans}")
            print(f"📣 Ответ LLM: {feedback}")
            print(f"✅ Балл: {score}/2")
            print("-" * 60)

            results.append((i, score))

    return student_id, total_score, results

# === 5. Проверка студентов с логом ===
results = []
for idx, row in csv_df.iterrows():
    student_id = str(row[ID_COLUMN_NAME]).strip()
    student_answers_raw = row.values[question_start_index:question_start_index + num_questions]
    student_answers = [str(ans).strip() if pd.notna(ans) else "" for ans in student_answers_raw]

    print(f"\n🔹 Проверка для студента {student_id}:")
    student_id, total_score, student_results = grade_student(
        student_id, ethalon_questions, ethalon_answers, student_answers
    )

    results.append({
        "ID студента": student_id,
        f"Оценка (из 10)": total_score
    })

# === 6. Вывод итогов
results_df = pd.DataFrame(results)
print("\n📊 Итоговая таблица:")
print(results_df)