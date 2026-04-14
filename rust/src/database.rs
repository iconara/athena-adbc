use std::sync::Arc;

use adbc_core::{
    Database, Optionable,
    error::Result,
    options::{OptionDatabase, OptionValue},
};
use tokio::runtime::Runtime;

use crate::connection::AthenaConnection;

pub struct AthenaDatabase {
    pub(crate) runtime: Arc<Runtime>,
}

impl Database for AthenaDatabase {
    type ConnectionType = AthenaConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(AthenaConnection::new(self.runtime.clone()))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

impl Optionable for AthenaDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Ok(())
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        todo!()
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        todo!()
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        todo!()
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        todo!()
    }
}
