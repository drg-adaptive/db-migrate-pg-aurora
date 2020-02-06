# Welcome to db-migrate-pg-aurora 👋
![stability](https://img.shields.io/badge/stability-alpha-red)
[![npm](https://img.shields.io/npm/v/db-migrate-aurora)](https://www.npmjs.com/package/db-migrate-pg-aurora)
[![Maintainability](https://api.codeclimate.com/v1/badges/eadef0c673311ff3ad10/maintainability)](https://codeclimate.com/github/drg-adaptive/db-migrate-pg-aurora/maintainability)
[![Build Status](https://travis-ci.org/drg-adaptive/db-migrate-aurora.svg)](https://travis-ci.org/drg-adaptive/db-migrate-pg-aurora)
[![Dependency Status](https://david-dm.org/drg-adaptive/db-migrate-aurora.svg)](https://david-dm.org/drg-adaptive/db-migrate-pg-aurora)
[![devDependency Status](https://david-dm.org/drg-adaptive/db-migrate-aurora/dev-status.svg)](https://david-dm.org/drg-adaptive/db-migrate-pg-aurora#info=devDependencies)

# AWS Postgres Aurora Serverless db-migrate Driver
> A driver for db-migrate to connect to PostgreSQL Aurora Serverless through the Data API

### 🏠 [Homepage](https://github.com/drg-adaptive/db-migrate-pg-aurora)

## Install

```sh
yarn install
```

## Usage

See [db-migrate](https://db-migrate.readthedocs.io/en/latest/) for more information.

### Configuration Example

Add the following configuration to your `database.json` file:

```json
{
  "prod": {
    "driver": {
      "require": "db-migrate-pg-aurora"
    },
    "database": "<Database Name>",
    "schema": "<Schema Name>",
    "secretArn": "<Secrets Manager ARN that contains credentials for the cluster> ",
    "resourceArn": "<Database Cluster ARN to connect to>",
    "region": "<AWS Region to connect to>",
    "maxRetries": 3,
    "connectTimeout": 45000
  }
}
```

## Run tests

```sh
yarn test
```

## Author

👤 **Elisha Boozer <eboozer@teamdrg.com>**

* Github: [@drg-adaptive](https://github.com/drg-adaptive)

## 🤝 Contributing

Contributions, issues and feature requests are welcome!

Feel free to check [issues page](https://github.com/drg-adaptive/db-migrate-pg-aurora/issues).

## Show your support

Give a ⭐️ if this project helped you!


## 📝 License

Copyright © 2019 [DRG Adaptive](https://drgadaptive.com/).

This project is [MIT](https://github.com/drg-adaptive/db-migrate-pg-aurora/blob/master/LICENSE) licensed.


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fdrg-adaptive%2Fdb-migrate-pg-aurora.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fdrg-adaptive%2Fdb-migrate-pg-aurora?ref=badge_large)

***
_This README was generated with ❤️ by [readme-md-generator](https://github.com/kefranabg/readme-md-generator)_
