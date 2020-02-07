import * as AWS from "aws-sdk";
import { CallbackFunction, ColumnSpec, ForeignKeyRules, InternalOptions } from "db-migrate-base";
import Bluebird = require("bluebird");
import semver = require("semver");

// @ts-ignore
const Promise = require("bluebird");
const BaseDriver = require("db-migrate-base");

export interface IInternalOptions extends InternalOptions {
  notransactions?: boolean;
  migrationTable?: string;
  seedTable?: string;
  dryRun?: boolean;
  rdsParams?: RDSParams;
  connection: AWS.RDSDataService;
  currentTransaction: string;
}

// @ts-ignore
interface IColumnSpec extends ColumnSpec {
  engine?: any;
  rowFormat?: any;
  onUpdate?: string;
  null?: boolean;
  name: string;
  length?: string | number;
}

interface ISwitchDatabaseOptions {
  database: string;
}

interface IDropDatabaseOptions {
  ifExists?: boolean;
}

export interface RDSParams {
  secretArn: string;
  resourceArn: string;
  database?: string;
  schema?: string;
  region: string;
  maxRetries?: number;
  connectTimeout?: number;
}

export default class AuroraDataApiDriver extends BaseDriver {
  _escapeDDL: string;
  _escapeString: string;

  constructor(private internals: IInternalOptions, rdsParams: RDSParams) {
    super(internals);

    console.debug(`Initializing driver...`);
    this._escapeDDL = `"`;
    this._escapeString = "'";

    this.internals.rdsParams = rdsParams;
    this.internals.connection = new AWS.RDSDataService({
      apiVersion: "2018-08-01",
      region: rdsParams.region,
      maxRetries: rdsParams.maxRetries !== undefined ? rdsParams.maxRetries : 3,
      httpOptions: {
        connectTimeout:
          rdsParams.connectTimeout !== undefined
            ? rdsParams.connectTimeout
            : 45000,
      },
    });
    this.internals.notransactions = false;
  }

  getConnection(): AWS.RDSDataService {
    console.debug(`Retrieving connection...`);
    return this.internals.connection;
  }

  startMigration(cb: CallbackFunction): Bluebird<any> {
    console.debug(`Starting migration...`);
    if (!this.internals.notransactions) {
      return Promise.cast(this.startTransaction).thenReturn().nodeify(cb);
    }
  }

  async startTransaction() {
    console.debug(`Initializing Transaction...`);
    const { transactionId } = await this.internals.connection
      .beginTransaction({
        resourceArn: this.internals.rdsParams.resourceArn,
        secretArn: this.internals.rdsParams.secretArn,
        database: this.internals.rdsParams.database,
        schema: this.internals.rdsParams.schema,
      })
      .promise();
    this.internals.currentTransaction = transactionId;
  }

  endMigration(cb: CallbackFunction): Bluebird<any> {
    console.debug(`Finishing migration...`);
    if (!this.internals.notransactions) {
      return Promise.cast(this.commitTransaction).thenReturn().nodeify(cb);
    }
  }

  async commitTransaction() {
    console.debug(`Committing Transaction...`);
    await this.internals.connection
      .commitTransaction({
        resourceArn: this.internals.rdsParams.resourceArn,
        secretArn: this.internals.rdsParams.secretArn,
        transactionId: this.internals.currentTransaction,
      })
      .promise();
    delete this.internals.currentTransaction;
  }

  mapDataType(str: string) {
    switch (str) {
      case "json":
      case "jsonb":
        return str.toUpperCase();
      case "string":
        return "TEXT";
      case "datetime":
        return "TIMESTAMP";
      case "blob":
        return "BYTEA";
    }

    return super.mapDataType(str);
  }

  createColumnDef(name: string, spec: any, options: any) {
    name = this._escapeDDL + name + this._escapeDDL;
    const type = this.mapDataType(spec.type);
    const len = spec.length ? `(${spec.length})` : "";
    const constraints = this.createColumnConstraint(spec, options).constraints;

    return {
      constraints: [name, type, len, constraints].join(" "),
    };
  }

  createColumnConstraint(
    spec: any,
    options?: any,
    tableName?: string,
    columnName?: string): any {
    const constraints: Array<string> = [];
    let cb;

    if (spec.timezone) {
      constraints.push("WITH TIME ZONE");
    }

    if (spec.notNull) {
      constraints.push("NOT NULL");
    }

    if (spec.defaultValue !== undefined) {
      constraints.push("DEFAULT");
      if (typeof spec.defaultValue === "string") {
        constraints.push(`'${spec.defaultValue}'`);
      } else if (typeof spec.defaultValue.prep === "string") {
        constraints.push(String(spec.defaultValue.prep));
      } else {
        constraints.push(String(spec.defaultValue));
      }
    }

    // keep foreignKey for backward compatible, push to callbacks in the future
    if (spec.foreignKey) {
      cb = this.bindForeignKey(tableName, columnName, spec.foreignKey);
    }

    return {
      foreignKey: cb,
      constraints: String(constraints.join(" ")),
    };
  }

  renameTable(tableName: string, newTableName: string): Bluebird<any> {
    const sql = `ALTER TABLE IF EXISTS "${tableName}" RENAME TO "${newTableName}"`;
    return this.runSql(sql);
  }

  createDatabase(dbName: string, options: any): Bluebird<any> {
    const createDBSQL = `CREATE DATABASE "${dbName}"`;
    if (options.ifNotExists) {
      return this.runSql(`CREATE EXTENSION IF NOT EXISTS dblink;
    DO $$
      BEGIN
        PERFORM dblink_exec('', '${createDBSQL}');
        EXCEPTION WHEN duplicate_database THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
      END
    $$;`);
    } else {
      return this.runSql(createDBSQL);
    }
  }

  dropDatabase(dbName: string, options?: IDropDatabaseOptions): Bluebird<any> {
    const ifExists = options.ifExists ? "IF EXISTS" : "";

    return this.runSql(`DROP DATABASE ${ifExists} "${dbName}"`);
  }

  createSequence(sqName: string, options?: any): Bluebird<any> {
    const temp = options?.temp ? "TEMP" : "";

    return this.runSql(`CREATE ${temp} SEQUENCE "${sqName}"`);
  }

  switchDatabase(options: ISwitchDatabaseOptions | string): Bluebird<any> {
    if (typeof options === "object" && typeof options.database === "string") {
      this.log.info(
        "Ignore database option, not available with postgres. Use schema instead!",
      );
      return this.runSql(`SET search_path TO "${options.database}"`);
    } else if (typeof options === "string") {
      return this.runSql(`SET search_path TO "${options}"`);
    }
  }

  dropSequence(dbName: string, options: any): Bluebird<any> {
    const ifExists = options.ifExists ? "IF EXISTS" : "";
    const rule = options.cascade ? "CASCADE" : options.restrict ? "RESTRICT" : "";

    return this.runSql(`DROP SEQUENCE ${ifExists} "${dbName}" ${rule}`);
  }

  removeColumn(tableName: string, columnName: string): Bluebird<any> {
    return this.runSql(`ALTER TABLE "${tableName}"
    DROP COLUMN IF EXISTS "${columnName}"`);
  }

  createMigrationsTable(cb: CallbackFunction): Bluebird<any> {
    const options = {
      columns: {
        id: {
          type: "SERIAL",
          notNull: true,
          primaryKey: true,
        },
        name: { type: "TEXT", notNull: true },
        run_on: { type: "TIMESTAMP", notNull: true },
      },
      ifNotExists: true,
    };

    console.debug("Creating migrations table (if necessary)...");
    return this.all("show server_version_num")
      .then((result: any) => {
        if (result && result.length > 0 && result[0].server_version_num) {
          let version = result[0].server_version_num;
          const major = Math.floor(version / 10000);
          const minor = Math.floor((version - major * 10000) / 100);
          const patch = Math.floor(version - major * 10000 - minor * 100);
          version = major + "." + minor + "." + patch;
          options.ifNotExists = semver.gte(version, "9.1.0");
        }

        // Get the current search path so we can change the current schema
        // if necessary
        return this.all("SHOW search_path");
      })
      // not all DBs support server_version_num, fall back to server_version
      .catch(() => {
        return this.all("show server_version").then(
          (result: any) => {
            if (result && result.length > 0 && result[0].server_version) {
              let version = result[0].server_version;
              // handle versions like “10.2 (Ubuntu 10.2)”
              version = version.split(" ")[0];
              // handle missing patch numbers
              if (version.split(".").length !== 3) {
                version += ".0";
              }
              options.ifNotExists = semver.gte(version, "9.1.0");
              // Get the current search path so we can change the current
              // schema if necessary
              return this.all("SHOW search_path");
            }
          },
        );
      }).then((result: any) => {
        let searchPath;
        const searchPaths = result[0].search_path.split(",");

        for (let i = 0; i < searchPaths.length; ++i) {
          if (searchPaths[i].indexOf("\"") !== 0) {
            searchPaths[i] = `"${searchPaths[i].trim()}"`;
          }
        }

        result[0].search_path = searchPaths.join(",");

        // if the user specified a different schema, prepend it to the search path.
        // This will make all DDL/DML/SQL operate on the specified schema.
        if (this.schema === "public") {
          searchPath = result[0].search_path;
        } else {
          searchPath = `"${this.schema}",${result[0].search_path}`;
        }
        return this.all("SET search_path TO " + searchPath);
      }).then(() => {
        return this.all(`SELECT table_name FROM information_schema.tables WHERE table_name = '${this.internals.migrationTable}' ${this.schema ? " AND table_schema = '${this.schema}'" : ""}`);
      }).then((result: any) => {
        if (result?.length < 1) {
          console.debug(`Creating migrations table with ${JSON.stringify(options)}...`);
          return this.createTable(this.internals.migrationTable, options);
        }
        console.debug("Found existing migrations table, no need to recreate.");
        return Promise.resolve();
      }).nodeify(cb);
  }

  createSeedsTable(cb: CallbackFunction): Bluebird<any> {
    const options = {
      columns: {
        id: {
          type: "SERIAL",
          notNull: true,
          primaryKey: true,
        },
        name: { type: "TEXT", notNull: true },
        run_on: { type: "TIMESTAMP", notNull: true },
      },
      ifNotExists: true,
    };

    return this.all("select version() as version")
      .then((result: any) => {
        if (result && result.length > 0 && result[0].version) {
          const version = result[0].version;
          const match = version.match(/\d+\.\d+\.\d+/);
          if (match && match[0] && semver.gte(match[0], "9.1.0")) {
            options.ifNotExists = true;
          }
        }

        // Get the current search path so we can change the current schema if necessary
        return this.all("SHOW search_path");
      }).then((result: any) => {
        let searchPath;

        // if the user specified a different schema, prepend it to the search path.
        // This will make all DDL/DML/SQL operate on the specified schema.
        if (this.schema === "public") {
          searchPath = result[0].search_path;
        } else {
          searchPath = `"${this.schema}",${result[0].search_path}`;
        }

        return this.all("SET search_path TO " + searchPath);
      }).then(() => {
        return this.all(`SELECT table_name FROM information_schema.tables WHERE table_name = '${this.internals.seedTable}' ${this.schema ? " AND table_schema = '${this.schema}'" : ""}`);
      }).then((result: any) => {
        if (result?.length < 1) {
          return this.createTable(this.internals.seedTable, options);
        }
        return Promise.resolve();
      }).nodeify(cb);
  }

  createTable(tableName: string, options: any): Bluebird<any> {
    console.log(`creating table: ${tableName}`);
    let columnSpecs = options;
    let opts;

    if (options.columns !== undefined) {
      columnSpecs = options.columns;
      opts = options;
    }

    let ifNotExistsSql = "";
    if (opts?.ifNotExists) {
      ifNotExistsSql = "IF NOT EXISTS";
    }

    const primaryKeyColumns = [];
    const columnDefOptions = {
      emitPrimaryKey: false,
    };

    for (let columnName in columnSpecs) {
      let columnSpec = this.normalizeColumnSpec(columnSpecs[columnName]);
      columnSpecs[columnName] = columnSpec;
      if (columnSpec.primaryKey) {
        primaryKeyColumns.push({ spec: columnSpec, name: columnName });
      }
    }

    let pkSql = "";
    if (primaryKeyColumns.length > 1) {
      pkSql = this._handleMultiPrimaryKeys(primaryKeyColumns);
    } else if (primaryKeyColumns.length === 1) {
      primaryKeyColumns[0] = primaryKeyColumns[0].name;
      columnDefOptions.emitPrimaryKey = true;
    }

    let columnDefs = [];
    let extensions = "";
    let tableOptions = "";

    for (let columnName in columnSpecs) {
      let columnSpec = columnSpecs[columnName];
      this._prepareSpec(columnName, columnSpec, columnDefOptions, tableName);
      let constraint = this.createColumnDef(
        columnName,
        columnSpec,
        columnDefOptions,
      );

      columnDefs.push(constraint.constraints);
    }

    const sql = `CREATE TABLE ${ifNotExistsSql} ${this.escapeDDL(tableName)} (${columnDefs.join(", ")}${extensions}${pkSql}) ${tableOptions}`;
    return this.runSql(sql);
  }

  addIndex(
    tableName: string,
    indexName: string,
    columns: string | Array<string | IColumnSpec>,
    unique?: boolean,
  ): Bluebird<any> {
    if (!Array.isArray(columns)) {
      columns = [columns];
    }

    const createIndexSQL = `CREATE ${unique ? "UNIQUE" : ""} INDEX IF NOT EXISTS "${indexName}"
    ON "${tableName}" (${columns.map((column) => (typeof column === "string" ? column : column.name)).join(", ")});`;
    return this.runSql(createIndexSQL);
  }

  removeIndex(tableName: string, indexName?: string): Bluebird<any> {
    // tableName is optional for other drivers, but required for mySql.
    // So, check the args to ensure they are valid
    if (!indexName) {
      throw new Error(
        "Illegal arguments, must provide \"tableName\" and \"indexName\"",
      );
    }

    return this.runSql(`DROP INDEX IF EXISTS "${indexName}";`);
  }

  async renameColumn(
    tableName: string,
    oldColumnName: string,
    newColumnName: string,
  ): Bluebird<any> {
    return this.runSql(`ALTER TABLE IF EXISTS "${tableName}"
    RENAME COLUMN "${oldColumnName}" TO "${newColumnName}";`);
  }

  async changeColumn(
    tableName: string,
    columnName: string,
    columnSpec: ColumnSpec & IColumnSpec,
  ) {
    if (columnSpec.notNull !== undefined) {
      const setOrDrop = columnSpec.notNull ? "SET" : "DROP";

      await this.runSql(`ALTER TABLE IF EXISTS "${tableName}"
    ALTER COLUMN "${columnName}" ${setOrDrop} NOT NULL`);
    }
    if (columnSpec.unique !== undefined) {
      const sql = columnSpec.unique
        ? `ALTER TABLE "${tableName}" ADD CONSTRAINT "${columnName}" UNIQUE (${columnName});`
        : `ALTER TABLE "${tableName}" DROP CONSTRAINT "${columnName}";`;
      await this.runSql(sql);
    }
    if (columnSpec.defaultValue !== undefined) {
      const sql = `ALTER TABLE "${tableName}"
    ALTER COLUMN "${columnName}" SET DEFAULT ${typeof columnSpec.defaultValue === "string"
        ? `'${columnSpec.defaultValue}'`
        : columnSpec.defaultValue}`;
      await this.runSql(sql);
    }
    if (columnSpec.type !== undefined) {
      const using = `USING "${columnName}"::${this.mapDataType(columnSpec.type)}`;
      const sql = `ALTER TABLE "${tableName}"
ALTER COLUMN "${columnName}" TYPE ${this.mapDataType(columnSpec.type)} ${using}`;
      await this.runSql(sql);
    }
  }

  protected addPrivateTableData(
    name: string,
    tableName: string,
    callback: (err: any, value?: any) => any,
  ): Bluebird<any> {
    return this.runSql(
      `INSERT INTO "${tableName}" (name, run_on) VALUES (:name, CURRENT_TIMESTAMP);`,
      [{ name: "name", value: { stringValue: name } }],
    ).then((result: any) => callback(undefined, result))
      .catch((err: any) => callback(err));
  }

  addMigrationRecord(
    name: string,
    callback: (err: any, value?: any) => any,
  ): Bluebird<any> {
    console.debug(`Adding Migration Record: ${name}`);
    return this.addPrivateTableData(
      name,
      this.internals.migrationTable,
      callback,
    );
  }

  /**
   * Deletes a migration
   *
   * @param migrationName   - The name of the migration to be deleted
   */
  async deleteMigration(migrationName: string, callback: CallableFunction) {
    const sql = `DELETE FROM "${this.internals.migrationTable}"  WHERE NAME = :name`;
    let result, error;

    try {
      result = this.runSql(sql, [
        {
          name: "name",
          value: {
            stringValue: migrationName,
          },
        },
      ]);
    } catch (ex) {
      error = ex;
    }

    if (callback) {
      return callback(error, result);
    } else if (error) {
      throw error;
    }

    return result;
  }

  addSeedRecord(
    name: string,
    callback: (err: any, value?: any) => any,
  ): Bluebird<any> {
    return this.addPrivateTableData(name, this.internals.seedTable, callback);
  }

  addForeignKey(
    tableName: string,
    referencedTableName: string,
    keyName: string,
    fieldMapping: any,
    rules: ForeignKeyRules | any = {},
  ): Bluebird<any> {
    const columns = Object.keys(fieldMapping);
    const referencedColumns = columns.map((key: string) => fieldMapping[key]);

    const sql = `ALTER TABLE "${tableName}"
      ADD CONSTRAINT "${keyName}"
          FOREIGN KEY (${this.quoteDDLArr(columns)})
              REFERENCES "${referencedTableName}" (${this.quoteDDLArr(referencedColumns)}) ON DELETE ${rules.onDelete || "NO ACTION"}`;

    return this.runSql(sql);
  }

  async removeForeignKey(
    tableName: string,
    keyName: string,
  ): Bluebird<any> {
    const sql = `ALTER TABLE "${tableName}"
    DROP CONSTRAINT "${keyName}"`;
    await this.runSql(sql);
  }

  insert() {
    let index = 1;

    if (arguments.length > 3) {
      index = 2;
    }

    arguments[index] = arguments[index].map((value: any) => {
      return typeof value === "string" ? value : JSON.stringify(value);
    });

    return this._super.apply(this, arguments);
  }

  async runSql(
    sql?: string,
    parameters?: Array<AWS.RDSDataService.SqlParameter>,
  ): Bluebird<any> {
    this.internals.mod.log.sql.apply(null, [sql, parameters]);

    if (this.internals.dryRun) {
      return Bluebird.resolve();
    }

    if (parameters?.length && sql.indexOf("?") >= 0) {
      parameters.forEach((value) => {
        sql = sql.replace("?", `:${value.name}`);
      });
    }

    const params: AWS.RDSDataService.ExecuteStatementRequest = {
      secretArn: this.internals.rdsParams.secretArn,
      resourceArn: this.internals.rdsParams.resourceArn,
      database: this.internals.rdsParams.database,
      schema: this.internals.rdsParams.schema,
      transactionId: this.internals.currentTransaction,
      includeResultMetadata: true,
      parameters,
      sql,
    };

    // @ts-ignore
    const exec = Bluebird.promisify(
      this.internals.connection.executeStatement,
    ).bind(this.internals.connection);

    let result;

    try {
      result = await exec(params).then(
        AuroraDataApiDriver.convertResultsToObjects,
      );
    } catch (ex) {
      console.error(`Error executing ${sql}: ${ex.message}`);
      throw ex;
    }

    return result;
  }

  private static convertResultsToObjects(
    data: AWS.RDSDataService.ExecuteStatementResponse,
  ): Array<any> {
    if (!data || !data.records) {
      return [];
    }

    return data.records.map((record: AWS.RDSDataService.Field[]) => {
      const result: { [key: string]: any } = {};

      data.columnMetadata.forEach(
        (value: AWS.RDSDataService.ColumnMetadata, idx: number) => {
          result[value.name] =
            record[idx].stringValue ||
            record[idx].longValue ||
            record[idx].doubleValue ||
            record[idx].booleanValue ||
            record[idx].blobValue;
        },
      );

      return result;
    });
  }

  // @ts-ignore
  async all(sql: string, callback?: Function): Bluebird<any> | any {
    let result;
    let error;
    try {
      result = await this.runSql(sql);
    } catch (ex) {
      error = ex;
    }

    if (callback) {
      callback(error, result);
    }

    if (error) {
      throw error;
    }

    return result;
  }

  close(): Bluebird<any> {
    return Bluebird.resolve();
  }
}
