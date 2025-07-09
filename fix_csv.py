# fix_csv.py
import csv

input_filename = 'corporate.csv'
output_filename = 'corporate_fixed.csv'

print(f"Reading from {input_filename} and writing to {output_filename}...")

with open(input_filename, 'r', newline='') as infile, \
        open(output_filename, 'w', newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in reader:
        if row:  # Make sure row is not empty
            domain = row[0]
            if not domain.startswith('http'):
                # Create a full URL
                full_url = f'https://{domain}'
                writer.writerow([full_url])  # write the new full URL
            else:
                # write the original row if it's already a URL
                writer.writerow(row)

print("\nDone! A new file 'corporate_fixed.csv' has been created.")
print("Now, replace the old file with the new one:")
print("  mv corporate_fixed.csv corporate.csv")
