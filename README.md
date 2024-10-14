# reis-finance
This is a personal finance app to build investiments portfolio dashboards from the broakers orders.
It leverages polars to process the orders raw data and build useful tables. 
The data visualization is out of scope of this app, it can print the tables on the terminal `--show`, but the main feature is, exporting the tables to a google sheet, where the visualizaitons can be built.

## How to configure
Generate an access token on trading212 web site. Instructions [here](https://helpcentre.trading212.com/hc/en-us/articles/14584770928157-How-can-I-generate-an-API-key).
Create a json file in `~/.config/reis-finance/trading212_config.json`
```json
{
  "token": "<trading 212 api token>",
  "starting_date": "2025-10-01T01:00:00.000Z"
 }
```

Generate a google api token as described [here](https://developers.google.com/identity/protocols/oauth2). At the end you should download a json file with the OAuth token.
Create another json file in `~/.config/reis-finance/google_config.json`:
```json
{
    "credentials_file" : "<path/to/google/token.json>",
    "spreadsheet_id" : "<id of the google sheet>",
    "spreadsheet_tab" : "<sheet tab name>",
    "spreadsheet_spacing" : 5
}
```

## How to run
```sh
reis-finance-cli --trading212-orders=<path/to/folder/to/store/orders> --update --chache --timeline 7
```

## How to build
```sh
nix develop
cargo build --release
```
