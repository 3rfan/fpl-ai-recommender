## Fantasy Premier League Recommender (Local ML-Powered Transfer Assistant)

A fully local **Fantasy Premier League (FPL)** data analytics and ML-powered recommendation engine that helps users decide the **best transfer to make for the next gameweek**.  
The application:

- Scrapes **fbref.com** for Premier League player statistics (last gameweek)  
- Fetches **official FPL player data** (positions + prices)  
- Builds a local PostgreSQL dataset  
- Computes features + applies ML/rules-based scoring  
- Suggests **top-N replacements per weak position** based on budget  
- Provides clear explanations and comparison charts via a React UI

---

## Tech Stack

### **Backend**
- Java 21
- Spring Boot 3
- REST API
- Maven
- YAML configuration
- PostgreSQL
- JUnit 5 (Test)
- Checkstyle (Personal)

### **Frontend**
- React
- Vite
- TypeScript
- Recharts (visuals)

### **Data Ingestion**
- Python 3 + BeautifulSoup4 scraper
- fbref.com dataset (last GW)
- Official FPL bootstrap-static JSON

---

## Running the Backend

### **1. Compile**
mvn clean install

### 2. Run
mvn spring-boot:run

### 3. Test endpoint
Open:
http://localhost:8080/api/test \
Expected output:
Backend is running!

### 4. Code Quality, Checkstyle

Run:
mvn verify \
Any violations will fail the build.
 
### 5. Test application (JUnit 5)
Run:
mvn test


---
## About the Project
This project is designed and developed to showcase the following skills:

- Machine Learning integration (AI)

- Full-stack development

- Data engineering

- Data scraping

- Code Testing & Quality

- Local deployment

- Real-world problem solving (FPL strategy)

More features will be added as development continues.
This README evolves with each milestone.