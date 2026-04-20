import pandas as pd
path = '/tmp/snies_downloads/matriculados_2024.xlsx'

xl = pd.ExcelFile(path)
print('=== HOJAS ===')
for i, name in enumerate(xl.sheet_names):
    print(f'  [{i}] {name!r}')

for sheet in xl.sheet_names:
    print(f'\n=== Hoja: {sheet!r} ===')
    df = pd.read_excel(path, sheet_name=sheet, header=None, nrows=15, dtype=str)
    print(f'Dimensiones (nrows=15): {df.shape}')
    for i, row in df.iterrows():
        values = [str(v)[:40] if pd.notna(v) else 'NaN' for v in row.values]
        print(f'  Fila {i}: {values}')