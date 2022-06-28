// all clippy warnings
#![warn(clippy::all)]

use std::{collections::HashMap, fmt::Display};

use error::{EngineError, RuntimeError, Error};
use parser::{PaymentCommand, parse_commands};

mod parser;
mod error;

pub type ClientId = u16;
pub type TransactionId = u16;

#[derive(Debug, Default)]
pub struct ClientData {
    available: f64,
    held: f64,
    locked: bool,
}

impl ClientData {
    pub fn total(&self) -> f64 {
        self.held + self.available
    }

    pub fn to_csv_string(&self, id: ClientId) -> String {
        format!(
            "{id},{},{},{},{}",
            self.available,
            self.held,
            self.total(),
            self.locked
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Ok,
    Disputed,
    Resolved,
    Chargeback,
}

#[derive(Debug)]
pub struct TransactionData {
    client: ClientId,
    amount: f64,
    status: TransactionStatus,
}

#[derive(Debug, Default)]
pub struct PaymentsEngine {
    clients: HashMap<ClientId, ClientData>,
    transactions: HashMap<TransactionId, TransactionData>,
}

impl PaymentsEngine {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    // get or insert default
    pub fn get_client_mut(&mut self, client: ClientId) -> &mut ClientData {
        self.clients
            .entry(client)
            .or_insert_with(ClientData::default)
    }

    /// Runs payment commands from a slice
    ///
    /// # Errors
    ///
    /// Will return any error from calling `PaymentsEngine::run_command`, wrapped in a `RuntimeError` with the correct line number.
    pub fn run_commands(&mut self, commands: &[PaymentCommand]) -> Result<(), RuntimeError> {
        for (i, command) in commands.iter().enumerate() {
            self.run_command(*command)
                .map_err(|e| RuntimeError(i + 2, e))?;
        }

        Ok(())
    }

    /// Runs a payment command
    ///
    /// # Errors
    ///
    /// - `ClietIdMismatch`: if the client id in a Dispute, Resolve, or Chargeback command differs from the client id in the transaction it references.
    pub fn run_command(&mut self, command: PaymentCommand) -> Result<(), EngineError> {
        match command {
            PaymentCommand::Deposit { client, tx, amount } => {
                // create transaction struct
                let transaction = TransactionData {
                    client,
                    amount,
                    status: TransactionStatus::Ok,
                };

                // add transaction
                self.transactions.insert(tx, transaction);

                // update client
                let client_data = self.get_client_mut(client);

                // increase available funds
                client_data.available += amount;
            }
            PaymentCommand::Withdrawal { client, tx, amount } => {
                // fail silently if account is locked not enough funds
                // it is ambiguous whether a withdrawal can occur on a frozen account, I have decided to assume it cannot.
                if self
                    .clients
                    .get(&client)
                    .map_or(true, |x| !x.locked && x.available < amount)
                {
                    return Ok(());
                }

                // create transaction struct
                let transaction = TransactionData {
                    client,
                    amount: -amount,
                    status: TransactionStatus::Ok,
                };

                // add transaction
                self.transactions.insert(tx, transaction);

                // update client
                let client_data = self.get_client_mut(client);

                // decrease available funds
                client_data.available -= amount;
            }
            PaymentCommand::Dispute { client, tx } => {
                match self.transactions.get_mut(&tx) {
                    Some(transaction_data)
                        if transaction_data.status != TransactionStatus::Disputed =>
                    {
                        if transaction_data.client != client {
                            return Err(EngineError::ClientIdMismatch(
                                client,
                                transaction_data.client,
                            ));
                        }

                        let amount = transaction_data.amount;

                        // change transaction status
                        transaction_data.status = TransactionStatus::Disputed;

                        let client_data = self.get_client_mut(client);

                        // update client funds
                        client_data.available -= amount;
                        client_data.held += amount;
                    }
                    // transaction does not exist or transaction already was disputed, fail silently
                    _ => (),
                }
            }
            PaymentCommand::Resolve { client, tx } => {
                match self.transactions.get_mut(&tx) {
                    Some(transaction_data)
                        if transaction_data.status == TransactionStatus::Disputed =>
                    {
                        if transaction_data.client != client {
                            return Err(EngineError::ClientIdMismatch(
                                client,
                                transaction_data.client,
                            ));
                        }

                        let amount = transaction_data.amount;

                        // change transaction status
                        transaction_data.status = TransactionStatus::Resolved;

                        let client_data = self.get_client_mut(client);

                        // update client funds
                        client_data.available += amount;
                        client_data.held -= amount;
                    }
                    // transaction does not exist or transaction is not disputed, fail silently
                    _ => (),
                }
            }
            PaymentCommand::Chargeback { client, tx } => {
                match self.transactions.get_mut(&tx) {
                    Some(transaction_data)
                        if transaction_data.status == TransactionStatus::Disputed =>
                    {
                        if transaction_data.client != client {
                            return Err(EngineError::ClientIdMismatch(
                                client,
                                transaction_data.client,
                            ));
                        }

                        let amount = transaction_data.amount;

                        // change transaction status
                        transaction_data.status = TransactionStatus::Disputed;

                        let client_data = self.get_client_mut(client);

                        // update client funds
                        client_data.held -= amount;
                        // freeze client
                        client_data.locked = true;
                    }
                    // transaction does not exist or transaction is not disputed, fail silently
                    _ => (),
                }
            }
        }

        Ok(())
    }

    /// Runs payment commands from a file
    ///
    /// # Errors
    ///
    /// Will return any errors it encounters from `parse_commands` and `PaymentsEngine::run_commands` or an IO Error from `std::fs::read_to_string` wrapped in a `crate::Error`.
    pub fn run_from_file(&mut self, path: &str) -> Result<(), Error> {
        let contents = std::fs::read_to_string(path).map_err(|e| format!("IO Error: {}", e))?;

        let commands = parse_commands(&contents)?;

        self.run_commands(&commands)?;

        Ok(())
    }

    // write the current state of the engine to a csv string
    pub fn to_csv_string(&self) -> String {
        let mut buf = String::new();

        buf.push_str("client,available,held,total,locked");

        for (id, data) in &self.clients {
            buf.push_str(&format!("\n{}", data.to_csv_string(*id)));
        }

        buf
    }

    // this is used for the tests, to ensure reproducible results
    pub fn to_csv_string_sorted(&self) -> String {
        let mut buf = String::new();

        buf.push_str("client,available,held,total,locked");

        let mut clients = self.clients.iter().collect::<Vec<_>>();
        clients.sort_by(|(x, _), (y, _)| x.cmp(y));

        for (id, data) in clients {
            buf.push_str(&format!("\n{}", data.to_csv_string(*id)));
        }

        buf
    }
}

impl Display for PaymentsEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_csv_string())
    }
}


// wrapper function to print the error message using Display instead of Debug
fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            // print error message
            eprintln!("{}", e);
            // exit with code 1 to signify an error
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(), Error> {
    // the very first argument is always the name of the program, so it can be skipped
    let mut args = std::env::args().skip(1);

    let filename = args
        .next()
        .ok_or_else(|| Error::Other("Please specify a file to run.".to_string()))?;

    let mut engine = PaymentsEngine::new();

    // run commands from the specified file.
    engine.run_from_file(&filename)?;

    // get output from the engine
    let output = engine.to_csv_string();

    // print to standard output
    print!("{output}");

    Ok(())
}

#[cfg(test)]
mod tests {
    macro_rules! gen_test {
        ($name:ident, should_err = $should_err:expr, $err_pat:pat = err) => {
            mod $name {
                use crate::*;

                #[test]
                pub fn test() -> Result<(), crate::Error> {
                    let name_str = stringify!($name);
                    let in_filename = format!("test_cases/{}_input.csv", name_str);
                    let out_filename = format!("test_cases/{}_output.csv", name_str);

                    let mut engine = PaymentsEngine::new();
                    let res = engine.run_from_file(&in_filename);

                    if $should_err {
                        assert!(res.is_err());

                        let err = res.unwrap_err();
                        println!("error: {:?}", err);
                        assert!(matches!(err, $err_pat));
                        return Ok(());
                    }

                    assert!(res.is_ok());

                    let expected_out = std::fs::read_to_string(out_filename)
                        .map_err(|e| Error::Other(format!("IO Error: {}", e)))?;

                    let engine_out = engine.to_csv_string_sorted();

                    assert_eq!(expected_out, engine_out);

                    Ok(())
                }
            }
        };

        ($name:ident, $err_pat:pat = err) => {
            gen_test!($name, should_err = true, $err_pat = err);
        };

        ($name:ident) => {
            gen_test!($name, should_err = false, _ = err);
        };
    }

    macro_rules! gen_tests {
        {$($name:ident $(, $err_pat:pat = err)?);*$(;)?} => {
            $(
                gen_test! { $name $(, $err_pat = err)? }
            )*
        };
    }

    gen_tests! {
        t0;
        t1;
        t2;
        t3;
        t4;
        t5, Error::Parse(parser::ParseError(0, parser::PaymentCommandParseError::MissingHeader(_))) = err;
        t6, Error::Runtime(RuntimeError(7, EngineError::ClientIdMismatch(2, 1))) = err;
    }
}
