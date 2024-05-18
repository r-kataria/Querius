# Querius

**Querius** is a pure Python in-memory SQL engine, featuring a custom-built lexer and parser developed from scratch. Designed as a learning project, Querius can help look inside how a SQL query can be processed.

> [!CAUTION]
> **Querius is intended solely for educational purposes and should not be used in production.** It lacks the optimizations and features of established SQL engines. For reliable and secure database management, please use proven SQL engines like PostgreSQL, MySQL, or SQLite.


## Use Cases

The only scenarios where Querius might be suitable are extremely niche environments, such as:

- Microcontrollers with an embedded Python Enviornment.
- Demonstrations to showcase the basics of SQL parsing and execution.
- Prototyping new SQL features before implementing them in a more established systems.

## Getting Started

To interact with Querius, run the `main.py` script and enter your SQL commands at the `sql>` prompt. Type `exit;` to terminate the session.

```bash
python main.py
