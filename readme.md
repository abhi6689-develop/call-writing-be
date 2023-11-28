### **Documentation for Frontend and Backend API**

#### **1. Introduction**
This project comprises a frontend and a backend component that work together to provide real-time options trading data and analytics. The backend, built in Python, interacts with financial market data sources via IBGateway to fetch real-time data. The frontend, developed using React, presents this data in a user-friendly interface, allowing for effective decision-making in options trading.

#### **2. Prerequisites**
To set up and run this application, you will need the following software and dependencies:

- **Node.js**: A JavaScript runtime for executing JavaScript on the server side.
- **Python**: The programming language used for the backend.
- **npm**: A package manager for JavaScript, used to install dependencies for the frontend.
- **pip**: A package manager for Python, used to install dependencies for the backend.
- **ibapi**: The Interactive Brokers API, necessary for the backend to fetch data from IBGateway.
- **IBGateway**: Software that allows automated trading solutions to connect to Interactive Brokers. Ensure it is installed and running for the backend to function properly.

#### **3. Getting Started**

##### **3.1. Cloning the Repositories**
First, clone the frontend and backend repositories:

**Backend Repository:**
```bash
git clone https://github.com/abhi6689-develop/call-writing-be.git
cd call-writing-be
```

**Frontend Repository:**
```bash
git clone https://github.com/abhi6689-develop/call-writing-fe.git
cd call-writing-fe
```

##### **3.2. Setting Up the Backend**
In the backend directory:

- **Virtual Environment (Windows)**:
  ```bash
  python -m venv venv
  .\venv\Scripts\activate
  ```
- **Virtual Environment (macOS/Linux)**:
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```
- **Install Dependencies**:
  ```bash
  pip install -r requirements.txt
  ```
- **Start the Server**:
  ```bash
  python server.py
  ```

##### **3.3. Setting Up the Frontend**
In the frontend directory:

- **Install Dependencies**:
  ```bash
  npm install
  ```
- **Start the Application**:
  ```bash
  npm start
  ```

By following these steps, you should have the backend and frontend up and running, allowing you to access real-time options trading data and analytics.