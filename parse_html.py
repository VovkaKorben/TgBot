import sqlite3
import io
import traceback
import os
import re


def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value) for idx, value in enumerate(row))


try:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    html_path = os.path.join(current_dir, "src")
    file_extension = ".html"
    file_names = [f for f in os.listdir(html_path) if f.endswith(file_extension)]
    print(f"Найдено файлов: {len(file_names)}")

    conn = sqlite3.connect("drugs.db")
    conn.row_factory = make_dicts

    for table_name in ["articles", "keywords", "headers"]:
        cursor = conn.execute(f"delete from {table_name}")
        conn.commit()

    for filename in file_names:

        with open(os.path.join(html_path, filename), "r", encoding="UTF-8") as htmlfile:
            html = htmlfile.read()

        cur = conn.execute("select IFNULL(max(drug_id),0) as drug_id from keywords")
        drug_id = cur.fetchall()
        cur.close()
        drug_id = drug_id[0]["drug_id"]

        header_id = -1
        matches = re.finditer(r"<(?P<tag>\w+)>(?P<value>.*?)<\/(?P=tag)>", html, re.I | re.DOTALL)
        for match in matches:
            tag = match.group(1).upper()
            value = match.group(2)
            if tag == "Q":
                cursor = conn.execute(f"INSERT into keywords (drug_id,drug_name) VALUES ({drug_id},'{value}')")
                conn.commit()
            elif tag == "H1":
                header_id += 1
                part_id = 0
                cursor = conn.execute(f"INSERT into headers (drug_id,header_id,header_text) VALUES ({drug_id},{header_id},'{value}')")
                conn.commit()
            elif tag == "P":
                cursor = conn.execute(f"INSERT into articles (drug_id,header_id,part_id,article) VALUES ({drug_id},{header_id},{part_id},'{value}')")
                conn.commit()
                part_id += 1

finally:
    if conn:
        conn.close()
