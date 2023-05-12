import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score, confusion_matrix
import os
import sys
import copy

random_seed = 1

# Load and sample the data
df_list = []
for i in range(10):
    df_temp = pd.read_csv(f'norm_sample{i}.csv').sample(frac=0.1, random_state=random_seed)
    df_list.append(df_temp)

# Concatenate dataframes into a single dataframe
df = pd.concat(df_list)

# Let's assume that the last column is the target variable
X = df.iloc[:, :-1]
y = df.iloc[:, -1]

# Convert continuous target variable into two categories
y = pd.cut(y, bins=2, labels=[0, 1])

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_seed)

# Initialize the DecisionTreeClassifier (CART)
clf = DecisionTreeClassifier(random_state=random_seed)

# Fit the model
clf.fit(X_train, y_train)

# Predict the labels
y_pred = clf.predict(X_test)

# Calculate and print the metrics
print("Precision: ", precision_score(y_test, y_pred, average='weighted'))
print("Recall: ", recall_score(y_test, y_pred, average='weighted'))
print("F1 Score: ", f1_score(y_test, y_pred, average='weighted'))
print("Accuracy: ", accuracy_score(y_test, y_pred))

print("\n")  # Add a newline for clearer output

# Print confusion matrix
print("Confusion Matrix: ")
print(confusion_matrix(y_test, y_pred))


def main(args):
    # check if path for weather data exists
    path = 'samples/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No sample directory found.")
        print("Place generated sample sets in 'samples/'.")
        input("Press enter to continue...")
        sys.exit()

    # check if number of sample sets and split percentage were passed
    if len(args) != 3:
        print("Arguments are not set right.")
        print("Run script as follows:")
        print("python script_name.py [number of samples] [split % as a decimal]")
        input("Press enter to continue...")
        sys.exit()

    # filenames to look for
    filename = path + 'norm_sampleX.csv'

    # number of sample sets
    try:
        num_sets = int(args[1])
    except ValueError:
        print("Invalid number of samples. Please provide a valid integer.")
        sys.exit(1)

    # split percentage
    try:
        split_percentage = float(args[2])
    except ValueError:
        print("Invalid split percentage. Please provide a valid decimal value.")
        sys.exit(1)

    # list to contain all the sample sets
    sample_list = []

    # load the sample sets if they exist
    for x in range(num_sets):
        s = copy.deepcopy(filename)
        s = s.replace('X', str(x))
        try:
            sample_list.append(pd.read_csv(s))
        except FileNotFoundError:
            print(f"Data not found for sample {x}. Please ensure all sample sets exist.")
            sys.exit(1)

# Call the main
    if __name__ == '__main__':
        main(sys.argv)




