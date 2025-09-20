# 🚛 Military Equipment Fault Analysis & Prediction Dashboard

An interactive **Streamlit-based dashboard** for analyzing, predicting, and visualizing faults in military vehicles and equipment.  
This project integrates **SQLite database**, **fault history**, **spare part usage**, and **predictive analytics** to assist in **maintenance decision-making**.

---

## 🔹 Features

- 📂 **Database Integration**  
  Uses SQLite (converted from NeonDB schemas) to store equipment, job cards, faults, and spare part transactions.  

- 📊 **Interactive Dashboard**  
  - View equipment details with filters (subcategory, unit, year).  
  - Fault summaries with spare part usage.  
  - Predictive maintenance forecasts.  
  - Pie charts, bar charts, scatter plots with Plotly.  

- 🛠 **Fault & Spare Analysis**  
  - Join across tables: `tEqptRecord → JobCard → JobCardDetails → tssTransactionRegister → tssStockMaster`.  
  - Concatenated spare parts per fault, grouped summaries, and history.  

- 🤖 **Machine Learning (Planned)**  
  - Predict vehicle breakdown risk & required spares (future release).  

- ☁️ **Streamlit Cloud Deployment**  
  - Deployable with one click from GitHub.  
  - Auto-generated public link for sharing.

---

## 🔧 Tech Stack

- **Frontend/UI:** Streamlit, Plotly  
- **Database:** SQLite3 (with CSV import support)  
- **Backend Logic:** Python (pandas, SQL joins, pyodbc/psycopg2 for NeonDB migration)  
- **Visualization:** Plotly (Pie, Bar, Scatter, Radar, 3D plots)  
- **Deployment:** Streamlit Cloud  

---

## 📂 Project Structure

📦 military-equipment-app
┣ 📜 app.py # Main Streamlit dashboard
┣ 📜 equipment_analytics.py # Analytics & chart functions
┣ 📜 db_utils.py # Database connection & queries
┣ 📜 requirements.txt # Dependencies for Streamlit Cloud
┣ 📜 README.md # Project documentation
┣ 📜 .gitignore # Ignore venv, db, cache files
┗ 📂 data # CSV files for database import


---

## 🚀 Deployment on Streamlit Cloud

1. Push your project to **GitHub**.  
2. Ensure your repo has:  
   - `app.py` (entry point)  
   - `requirements.txt`  
   - Database file or CSVs (inside `/data`)  
3. Go to [https://military-equipment.streamlit.app/]
4. Create a new app → Select repo & branch → Set entry file to `app.py`.  
5. 🎉 Done! You’ll get a public URL like:  



---

## 📦 Installation (Local Development)

```bash
# Clone repo
git clone https://github.com/Sanjoy-Chattopadhay/military-equipment-app.git
cd military-equipment-app

# Create virtual environment
python -m venv .venv
source .venv/bin/activate   # (Linux/Mac)
.venv\Scripts\activate      # (Windows)

# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run deployApp.py

👨‍💻 Author

Sanjoy Chattopadhay
📌 Project: Military Vehicle Fault Analysis using Streamlit & SQLite
📌 GitHub: Sanjoy-Chattopadhay
