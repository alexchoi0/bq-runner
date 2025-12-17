use std::collections::HashMap;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::executor::{BigQueryExecutor, Executor, ExecutorMode, QueryResult, YachtSqlExecutor};
use crate::rpc::types::{DagTableDef, DagTableDetail, DagTableInfo};

use super::dag::Dag;

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, Session>>,
    mode: ExecutorMode,
}

struct Session {
    executor: Executor,
    dag: Dag,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode: ExecutorMode::Mock,
        }
    }

    pub fn with_mode(mode: ExecutorMode) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode,
        }
    }

    pub async fn create_session(&self) -> Result<Uuid> {
        let session_id = Uuid::new_v4();
        let executor = match self.mode {
            ExecutorMode::Mock => Executor::Mock(YachtSqlExecutor::new()?),
            ExecutorMode::BigQuery => Executor::BigQuery(BigQueryExecutor::new().await?),
        };
        let dag = Dag::new();

        let session = Session { executor, dag };

        self.sessions.write().insert(session_id, session);

        Ok(session_id)
    }

    pub fn destroy_session(&self, session_id: Uuid) -> Result<()> {
        self.sessions
            .write()
            .remove(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;

        Ok(())
    }

    pub fn execute_query(&self, session_id: Uuid, sql: &str) -> Result<QueryResult> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.executor.execute_query(sql)
    }

    pub fn execute_statement(&self, session_id: Uuid, sql: &str) -> Result<u64> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.executor.execute_statement(sql)
    }

    pub fn register_dag(
        &self,
        session_id: Uuid,
        tables: Vec<DagTableDef>,
    ) -> Result<Vec<DagTableInfo>> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.dag.register(tables)
    }

    pub fn run_dag(
        &self,
        session_id: Uuid,
        targets: Option<Vec<String>>,
    ) -> Result<Vec<String>> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.dag.run(&session.executor, targets)
    }

    pub fn get_dag(&self, session_id: Uuid) -> Result<Vec<DagTableDetail>> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        Ok(session.dag.get_tables())
    }

    pub fn clear_dag(&self, session_id: Uuid) -> Result<()> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.dag.clear(&session.executor);
        Ok(())
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let result = manager.execute_query(session_id, "SELECT 1 AS x");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_multiple_sessions() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();
        let s3 = manager.create_session().await.unwrap();

        assert_ne!(s1, s2);
        assert_ne!(s2, s3);
    }

    #[tokio::test]
    async fn test_destroy_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        assert!(manager.execute_query(session_id, "SELECT 1").is_ok());
        manager.destroy_session(session_id).unwrap();
        assert!(manager.execute_query(session_id, "SELECT 1").is_err());
    }

    #[tokio::test]
    async fn test_destroy_nonexistent_session() {
        let manager = SessionManager::new();
        let fake_id = Uuid::new_v4();

        let result = manager.destroy_session(fake_id);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_isolation() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE users (id INT64, name STRING)")
            .unwrap();

        manager
            .execute_statement(s1, "INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE users (id INT64, name STRING)")
            .unwrap();

        manager
            .execute_statement(s2, "INSERT INTO users VALUES (2, 'Bob'), (3, 'Charlie')")
            .unwrap();

        let result1 = manager
            .execute_query(s1, "SELECT COUNT(*) FROM users")
            .unwrap();

        let result2 = manager
            .execute_query(s2, "SELECT COUNT(*) FROM users")
            .unwrap();

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);

        let count1: i64 = result1.rows[0][0].as_i64().unwrap();
        let count2: i64 = result2.rows[0][0].as_i64().unwrap();

        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
    }
}
