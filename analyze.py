import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


conn = sqlite3.connect('interactions.db')


df = pd.read_sql_query("SELECT * FROM interactions", conn)


conn.close()


print("Sample data:")
print(df.head())


interaction_counts = df['interaction_type'].value_counts()
print("\nInteraction type counts:")
print(interaction_counts)


interaction_counts.plot(kind='bar', color='skyblue', title='Interaction Type Distribution')
plt.xlabel('Interaction Type')
plt.ylabel('Count')
plt.tight_layout()
plt.show()
