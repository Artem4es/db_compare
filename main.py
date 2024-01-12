import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Optional

from pymysql import connect
from sqlalchemydiff import compare
from sqlalchemydiff.comparer import CompareResult

DEFAULT_MIGRATION_FILE_PREFIX: str = "migration_"


class DBReviser:
    """Класс для сверки схем БД и применения миграций"""

    def __init__(self, db_user: str, db_host: str, left_db_name: str, right_db_name: str, db_pass, migration_file: str):
        self.left_db_uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{left_db_name}"
        self.right_db_uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{right_db_name}"
        self.right_db_name = right_db_name
        self.left_db_connection = connect(host=db_host, user=db_user, password=db_pass, db=left_db_name)
        self.right_db_connection = connect(host=db_host, user=db_user, password=db_pass, db=right_db_name)
        self.migration_file = migration_file

    def read_db_schema(self, tables_to_add: list) -> dict:
        """Читаем схему целевой БД"""
        tables_info = defaultdict(list)
        with self.left_db_connection.cursor() as curr:
            for table in tables_to_add:
                curr.execute(f"DESCRIBE {table}")
                for row in curr:
                    tables_info[table].append(row)

        return tables_info

    def create_table_script(self, table_name: str, columns: list) -> str:
        """Генерируем скрипт создания таблицы"""
        column_defs = list()
        for col_name, col_type, nullable, key, default, extra in columns:
            col_def = f"{col_name} {col_type}"
            if nullable == "NO":
                col_def += " NOT NULL"

            if key:
                if key == "PRI":
                    col_def += " PRIMARY KEY"

                if key == "UNI":
                    col_def += " UNIQUE"

            if default is not None:
                col_def += f" DEFAULT '{default}'"

            if extra:
                col_def += f" {extra}"

            column_defs.append(col_def)

        column_defs_str = ", ".join(column_defs)
        return f"CREATE TABLE {self.right_db_name}.{table_name} ({column_defs_str});"

    def create_tables_ddl(self, tables_info: dict) -> str:
        """Читаем свойства таблиц и генерируем скрипты миграций"""
        migration_ddl = list()
        for table, info in tables_info.items():
            table_ddl: str = self.create_table_script(table, info)
            migration_ddl.append(table_ddl)

        return "\n".join(migration_ddl)

    def process_schema_change(self, tables: dict) -> str:
        """Проверяем изменения в схеме БД"""
        migration_ddl = list()
        tables_to_add: Optional[list] = tables.get("left_only")
        tables_to_remove: Optional[list] = tables.get("right_only")

        if tables_to_add:
            tables_info: dict = self.read_db_schema(tables_to_add)
            migration_script: str = self.create_tables_ddl(tables_info)
            migration_ddl.append(migration_script)

        if tables_to_remove:
            drop_script = list()
            for table in tables_to_remove:
                drop_script.append(f"DROP table {self.right_db_name}.{table};")

            migration_ddl.append("\n".join(drop_script))

        return "\n".join(migration_ddl)

    def process_diff_columns_change(self, left_only: list, left_primary_keys: list, table_name: str) -> str:
        """Обработка новых полей"""
        sql_commands = list()
        for field in left_only:
            pk = False
            for primary_key in left_primary_keys:
                if primary_key == field["name"]:
                    pk = True
            sql_commands.append(self.alter_table_writer(table_name, field, "ADD", pk))

        return "\n".join(sql_commands)

    def process_same_columns_change(self, differences: list, left_primary_keys: list, table_name: str) -> str:
        """Обработка полей, изменивших свойства (одинаковое название)"""
        sql_commands = list()
        for field in differences:
            left_field: dict = field.get("left")
            pk = False

            for primary_key in left_primary_keys:
                if primary_key == left_field["name"]:
                    pk = True

            sql_commands.append(self.alter_table_writer(table_name, left_field, "MODIFY", pk))

        return "\n".join(sql_commands)

    def process_tables_change(self, diff: dict) -> str:
        """Обработка изменения таблиц"""
        sql_commands = list()
        for table_name, changes in diff.items():
            columns: Optional[dict] = changes.get("columns")
            indexes: dict = changes.get("indexes", {})
            primary_keys: dict = changes.get("primary_keys", {})
            foreign_keys: dict = changes.get("foreign_keys", {})
            if columns:
                sql_commands.append(self.process_column_change(columns, primary_keys, table_name))

            if indexes:
                sql_commands.append(self.process_index_change(indexes, table_name))

            if primary_keys:
                sql_commands.append(self.process_primary_keys(primary_keys, table_name))

            if foreign_keys:
                sql_commands.append(self.process_foreign_keys(foreign_keys, table_name))

        return "\n".join(sql_commands)

    def process_column_change(self, columns: dict, primary_keys: dict, table_name: str) -> str:
        """Обрабатывает изменения полей таблицы"""
        sql_commands = list()
        right_only: list = columns.get("right_only", [])
        for column in right_only:
            sql_commands.append(f"ALTER TABLE {self.right_db_name}.{table_name} DROP COLUMN {column['name']};")

        left_only: list = columns.get("left_only", [])
        left_primary_keys: list = primary_keys.get("left_only", [])
        sql_commands.append(self.process_diff_columns_change(left_only, left_primary_keys, table_name))
        differences: list = columns.get("diff", [])
        sql_commands.append(self.process_same_columns_change(differences, left_primary_keys, table_name))

        return "\n".join(sql_commands)

    def process_index_change(self, indexes: dict, table_name: str) -> str:
        """Обрабатывает добавление / удаление индексов"""
        sql_commands = list()
        right_indexes: list = indexes.get("right_only", [])
        left_indexes: list = indexes.get("left_only", [])

        for index in right_indexes:
            index_name: str = index["name"]
            field_names: list = index["column_names"]
            if len(field_names) > 1:
                raise NotImplementedError("Пока не умею обрабатывать индексы со ссылками на несколько полей")

            sql_commands.append(f"DROP INDEX {index_name} ON {self.right_db_name}.{table_name};")

        for index in left_indexes:
            index_name: str = index["name"]
            field_names: list = index["column_names"]
            if len(field_names) > 1:
                raise NotImplementedError("Пока не умею обрабатывать индексы со ссылками на несколько полей")

            field_name: str = field_names[0]
            unique: bool = index["unique"]
            sql_commands.append(
                f"CREATE {'UNIQUE' if unique else ''} INDEX {index_name} ON {self.right_db_name}.{table_name}({field_name});"
            )

        return "\n".join(sql_commands)

    def process_primary_keys(self, primary_keys: dict, table_name: str) -> str:
        """Обработка удаления первичных ключей"""
        sql_commands = list()
        right_pk: list = primary_keys.get("right_only", [])
        for pkey in right_pk:
            sql_commands.append(f"ALTER TABLE {self.right_db_name}.{table_name} DROP PRIMARY KEY;")

        return "\n".join(sql_commands)

    def process_foreign_keys(self, foreign_keys: dict, table_name: str) -> str:
        """Обработка внешних ключей"""
        sql_commands = list()
        right_fk: list = foreign_keys.get("right_only", [])
        left_fk: list = foreign_keys.get("left_only", [])
        for pkey_name in right_fk:
            fk_name: str = pkey_name["name"]
            ref_columns: list = pkey_name["referred_columns"]
            if len(ref_columns) > 1:
                raise NotImplementedError("Пока не умею обрабатывать FK ссылающиеся на более чем 1 поле")

            sql_commands.append(f"ALTER TABLE {self.right_db_name}.{table_name} DROP FOREIGN KEY {fk_name};")

        for pkey_name in left_fk:
            fk_name: str = pkey_name["name"]
            fk_fields: list = pkey_name["constrained_columns"]
            if len(fk_fields) > 1:
                raise NotImplementedError("Пока не умею обрабатывать FK ссылающиеся на более чем 1 поле")

            fk_field: str = fk_fields[0]
            ref_table: str = pkey_name["referred_table"]
            ref_columns: list = pkey_name["referred_columns"]
            if len(ref_columns) > 1:
                raise NotImplementedError("Пока не умею обрабатывать FK ссылающиеся на более чем 1 поле")

            ref_column: str = ref_columns[0]
            sql_commands.append(
                f"ALTER TABLE {self.right_db_name}.{table_name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk_field}) REFERENCES {self.right_db_name}.{ref_table}({ref_column});"
            )

            return "\n".join(sql_commands)

    def alter_table_writer(self, table_name: str, left_field: dict, col_action: str, pk: bool) -> str:
        """Формирование SQL скрипта создания таблиц"""
        col_def = f"ALTER TABLE {self.right_db_name}.{table_name}"
        col_def += f" {col_action} COLUMN {left_field['name']} {left_field['type']}"
        col_def += f" PRIMARY KEY" if pk else ""
        col_def += f" AUTO_INCREMENT" if left_field.get("autoincrement") else ""
        col_def += f" NOT NULL" if not left_field.get("nullable") else ""
        col_def += f" DEFAULT {left_field.get('default')}" if left_field.get("default") is not None else ""
        col_def += f" COMMENT '{left_field.get('comment')}'" if left_field.get("comment") else ""
        return f"{col_def};"

    def compare_schemas(self) -> bool:
        need_second_run: bool = False
        migration_ddl = list()
        diff: Optional[dict] = self.get_differences()

        if not diff:
            print(f"{'-'*100} \n Базы данных идентичны")
            return need_second_run

        # проверяем удаление/создание новых таблиц
        schema_changed: dict = diff["tables"]

        # проверяем изменение структуры таблиц
        columns_changed: dict = diff["tables_data"]

        if schema_changed:
            need_second_run = True
            migration_script: str = self.process_schema_change(schema_changed)
            migration_ddl.append(migration_script)

        if columns_changed:
            need_second_run = True
            migration_script: str = self.process_tables_change(columns_changed)
            migration_ddl.append(migration_script)

        print("-" * 100, "Созданы следующие миграции:", "\n".join(migration_ddl), sep="\n", end="\n")
        self.create_migration_file(migration_ddl)
        return need_second_run

    def get_differences(self) -> Optional[dict]:
        """Сравнение всей схемы БД"""
        result: CompareResult = compare(self.left_db_uri, self.right_db_uri)
        if result.is_match:
            return

        return result.errors

    def create_migration_file(self, migration_ddl: list) -> None:
        name: str = self.migration_file + datetime.now().strftime("%Y-%m-%d-%H-%M")
        with open(name, "w") as file:
            file.write("\n".join(migration_ddl))

    @staticmethod
    def read_migration(migration_file_path: str) -> Optional[list]:
        with open(migration_file_path, "r") as file:
            migration_ddl: list = file.readlines()

        migration_ddl = list(map(lambda a: a.replace("\n", ""), migration_ddl))
        return migration_ddl

    @staticmethod
    def find_latest_migration_file() -> str:
        """Найти последний файл миграций"""
        pattern = re.compile(rf"{DEFAULT_MIGRATION_FILE_PREFIX}(\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}})")
        latest_time = None
        latest_file = None

        for file in os.listdir("."):
            match = pattern.match(file)

            if match:
                file_time = datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M")

                if not latest_time or file_time > latest_time:
                    latest_time = file_time
                    latest_file = file

        return latest_file

    def apply_migrations(self, next_message: Optional[str] = None):
        last_migration: Optional[str] = self.find_latest_migration_file()
        if last_migration:
            message = (
                f"\n Вы собираетесь внести изменения в существующую БД. Внимательно ознакомьтесь с применяемыми миграциями"
                f" из файла {last_migration}. \n Чтобы ознакомиться с миграциями из файла нажмите n. "
                f"Для тестирования изменений настоятельно рекомендуем создать новую БД и протестировать "
                f"миграции на ней! \n Если вы всё равно хотите применить миграции к базе данных '{self.right_db_name}' нажмите y/д"
            )
            if next_message:
                print("-" * 100)
                message = next_message.format(last_migration)

            inp: bool = True if input(message).lower() in ("y", "д") else False
            if inp:
                migration_script: list = self.read_migration(last_migration)
                with self.right_db_connection.cursor() as curr:
                    for script in migration_script:
                        if script:
                            curr.execute(script)

                second_run: bool = self.compare_schemas()
                if second_run:
                    self.apply_migrations("Создан дополнительный файл миграций {}, продолжить? y/n")

                return

        print("\n Не было применено ни одной миграции")


def main():
    migration_dir = os.path.abspath(".")
    migration = os.path.join(migration_dir, DEFAULT_MIGRATION_FILE_PREFIX)
    reviser = DBReviser("dbeaver", "localhost", "dev", "prod", "dbeaver", migration)

    if reviser.compare_schemas():
        reviser.apply_migrations()


if __name__ == "__main__":
    main()
