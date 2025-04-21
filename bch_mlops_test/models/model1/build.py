

def load_data():
    """Load the dataset."""
    global X_train, X_test, y_train, y_test
    logger.info(f"Loading Data from {DATA_PATH}...")

    # Hard-code the path to your project root
    project_root = '/builds/dana_george_trial/mlops_gitlab_snowflake/.gitlab/ci'

    # Add the project root to Python's module search path and get global variables
    sys.path.append(project_root)
    from data.processed.sf_data_processed import get_processed_dataframe, set_processed_dataframe

    # Get the session DataFrame
    sf_data_processed = get_processed_dataframe()
    df = sf_data_processed

    # Train the Model
    # Split the data into features (X) and target (y)
    X = df.drop(columns=['ALOHA_PREDICTION', 'LOS', 'LEVEL_0', 'ENC_ID'])
    y = df['LOS']

    return X, y

def preprocess_data(X, y):
    """Preprocess the data."""
    logger.info("Preprocessing data")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    return X_train_scaled, X_test_scaled, y_train, y_test, scaler

def train_model(X_train, y_train):
    """Train the model."""
    logger.info("Training model")
    model = RandomForestClassifier(n_estimators=100, random_state=42)  # Adjust model and hyperparameters as needed
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    """Evaluate the model."""
    logger.info("Evaluating model")
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"Model accuracy: {accuracy:.4f}")
    logger.info("\nClassification Report:\n" + classification_report(y_test, y_pred))
    return accuracy

def save_model(model, scaler):
    """Save the model and scaler."""
    logger.info(f"Saving model to {MODEL_PATH}")
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump({'model': model, 'scaler': scaler}, MODEL_PATH)

def main():
    """Main function to orchestrate the model building process."""
    logger.info("Starting model building process")
    
    X, y = load_data()
    X_train, X_test, y_train, y_test, scaler = preprocess_data(X, y)
    model = train_model(X_train, y_train)
    accuracy = evaluate_model(model, X_test, y_test)
    save_model(model, scaler)
    
    logger.info("Model building process completed")
    return accuracy

if __name__ == "__main__":
    main()
