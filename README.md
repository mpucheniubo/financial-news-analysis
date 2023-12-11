# 📊 Financial News Analysis

This is a small and quick implementation to find the keywords from German financial news portals that uses `Spacy` and runs as an Azure function.

## ⚙️ Functionality

The function is trigger via an HTTP request that contains the unique identifier of the news article that has been previously stored in the database. The information is retrieved and the NLP model used to determine the keywords with the respective scores. Some words that happened to appear too often are being explicitly excluded.

## 📜 Notes

This was part of a larger project that never went into production, so a cleaner implementation with a more TDD approach won't happen.

The project is being made public without the git history.