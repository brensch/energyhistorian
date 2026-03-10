use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::schema::{ObservedSchema, SchemaApprovalStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrationOutcome {
    pub inserted: usize,
    pub updated_last_seen: usize,
    pub total_registered: usize,
}

#[derive(Debug, Clone)]
pub struct FileSchemaRegistry {
    path: PathBuf,
}

impl FileSchemaRegistry {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn register(
        &self,
        observed_schemas: &[ObservedSchema],
    ) -> Result<SchemaRegistrationOutcome> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut existing = self.load_all()?;
        let mut index = existing
            .iter()
            .enumerate()
            .map(|(idx, item)| (item.schema_id.clone(), idx))
            .collect::<HashMap<_, _>>();

        let mut inserted = 0;
        let mut updated_last_seen = 0;
        for observed in observed_schemas {
            if let Some(existing_idx) = index.get(&observed.schema_id).copied() {
                let current = &mut existing[existing_idx];
                current.last_seen_at = observed.last_seen_at;
                current.observed_in_artifact_id = observed.observed_in_artifact_id.clone();
                if matches!(current.approval_status, SchemaApprovalStatus::Rejected) {
                    current.approval_status = SchemaApprovalStatus::Proposed;
                }
                updated_last_seen += 1;
            } else {
                index.insert(observed.schema_id.clone(), existing.len());
                existing.push(observed.clone());
                inserted += 1;
            }
        }

        fs::write(&self.path, serde_json::to_vec_pretty(&existing)?)?;
        Ok(SchemaRegistrationOutcome {
            inserted,
            updated_last_seen,
            total_registered: existing.len(),
        })
    }

    pub fn load_all(&self) -> Result<Vec<ObservedSchema>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&self.path)?;
        let schemas = serde_json::from_slice(&bytes)?;
        Ok(schemas)
    }
}
