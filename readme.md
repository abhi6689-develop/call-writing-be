## **Documentation for Frontend and Backend API**

### **1. Introduction**
- Brief overview of the project.
- Purpose and main functionality of the application.

### **2. Prerequisites**
- Required software and versions: 
  - Node.js
  - Python
- Any global dependencies or tools needed:
  - npm
  - pip
  - **ibapi**: It's essential for the backend to fetch data. Ensure you have `ibapi` installed.
- **IBGateway**: Ensure that you have IBGateway installed and running. The backend fetches real-time data using this gateway.

### **3. Getting Started**

#### **3.1. Cloning the Repositories**
To begin, clone both the frontend and backend repositories.

**In the first terminal session:**
```bash
# Clone the backend repository
git clone https://github.com/abhi6689-develop/call-writing-be.git

# Navigate to the backend directory
cd call-writing-be
```

**In a separate second terminal session:**
```bash
# Clone the frontend repository
git clone https://github.com/abhi6689-develop/call-writing-fe.git

# Navigate to the frontend directory
cd call-writing-fe
```

#### **3.2. Setting Up the Backend**
- **Dependencies**: Install all required Python packages using: `pip install -r requirements.txt`.
- **Starting the Server**: Use the command: `python server.py`.

#### **3.3. Setting Up the Frontend**
- **Dependencies**: Install all required Node modules using: `npm install`.
- **Starting the Application**: Use the command: `npm start`.
