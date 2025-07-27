# cdss-dev-project
Developing a CDSS as a mini project

## Project Structure

```bash
cdss-dev-project/
│
├── data/
│   ├── project_db.xlsx       # Patients batch file
│   ├── cdss.db               # Processed DB for all operations. Created automatically.
│   └── Loinc_2.80.zip
│
├── backend/
├── ├── queries/              # All of the queries for initialization, data access and business logic
│   │   ├── create_patients_table.sql
│   │   ├── create_loinc_table.sql
│   │   ├── insert_patient.sql
│   │   └── ...
│   │
├── ├── tak/                  # All abstraction and rules TAK/ Json files.
│   │   ├── hemoglobin_state.xml
│   │   ├── wbc_state.xml
│   │   └── ...
│   │
│   ├── backend_config.py     # Configuration file for the backend operations
│   ├── dataaccess.py         # DB connection module
│   ├── businesslogic.py      # Business logic module
│   ├── mediator.py           # Data abstraction calculation module
│   └── engine.py             # Rule-based patient's state inference module
│
├── frontend/
│   ├── images/               # Images used for design
│   └── userinterface.py      # UI - Data management system + recommendation board
│
├── README.md
└── requirements.txt
```

## Installation
### Prerequisites

- Python 3.7 or higher.
- Loinc_2.80.zip (Downloaded from [Loinc-Org](https://loinc.org/downloads/)) - Should be placed under `data` repository.

NOTE: For the recommendation system section of thsi project, the LOINC table was modified, re-zipped and used, adding a few SNOMED codes to it for the engine's rules. 

### Setup

1. Clone the repository:

```bash
git clone https://github.com/shaharoded/cdss-dev-project.git
cd cdss-dev-project
```

2. Create and activate a virtual environment:

```bash
# On Windows
python -m venv venv
.\venv\Scripts\Activate

# On macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## How to use
### Backend Tests

Once you updates the __main__ of each module:

You can run the folowing PowerShell command in the root repository to initiate the DataAccess class and build your DB:
```bash
(from root)
python -m backend.dataaccess
```

You can run the folowing PowerShell command in the root repository to test your business logic:
```bash
(from root)
python -m backend.businesslogic
```

You can run the folowing PowerShell command in the root repository to test your abstraction logic:
```bash
(from root)
python -m backend.mediator
```

Need to remove the DB you created?

```bash
rm cdss.db
```

### Run APP
In order to run the application, you'll need to navigate to the frontend folder and run the code from there:

```bash
(from root)
cd frontend
python userinterface.py
```

## GIT Commit Tips
Once you've made changes, commit and push as usual:

```bash
git add .
git commit -m "Commit message"
git push -u origin main
```
