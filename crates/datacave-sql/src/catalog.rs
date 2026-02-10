use datacave_core::catalog::Catalog;
use std::sync::{Arc, Mutex};

pub type SharedCatalog = Arc<Mutex<Catalog>>;
