import {
  AthenaClient,
  GetQueryExecutionCommand,
  StartQueryExecutionCommand,
  GetQueryResultsCommand,
} from "@aws-sdk/client-athena";

interface AthenaSdkConfig {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  database: string;
  workgroup: string;
  catalog?: string | null;
}

interface AthenaQueryReuseOptions {
  enabled: boolean;
  maxAgeInMinutes: number;
}

interface AthenaQuery {
  query: string;
  reuse?: AthenaQueryReuseOptions;
}

export class AthenaSdk {
  private readonly config: AthenaSdkConfig;
  private readonly awsAthenaInstance: AthenaClient;
  private readonly s3BucketName: string;

  constructor(config: AthenaSdkConfig) {
    this.config = {
      ...config,
      workgroup: config.workgroup ?? "primary",
      catalog: config.catalog ?? null,
    };

    this.awsAthenaInstance = new AthenaClient({
      region: this.config.region,
      credentials: {
        accessKeyId: this.config.accessKeyId,
        secretAccessKey: this.config.secretAccessKey,
      },
    });
  }

  public async runQuery(athenaQuery: AthenaQuery): Promise<any> {
    try {
      const input = {
        QueryString: athenaQuery.query,
        QueryExecutionContext: {
          Database: this.config.database,
          Catalog: this.config.catalog,
        },
        ResultConfiguration: {
          OutputLocation: this.s3BucketName,
        },
        WorkGroup: this.config.workgroup,
        ResultReuseConfiguration: {
          ResultReuseByAgeConfiguration: {
            Enabled: athenaQuery.reuse.enabled ?? false,
            MaxAgeInMinutes: Number(athenaQuery.reuse.maxAgeInMinutes) ?? 0,
          },
        },
      };
      const command = new StartQueryExecutionCommand(input);
      const response = await this.awsAthenaInstance.send(command);

      return this.awaitQuery(response.QueryExecutionId);
    } catch (error) {
      console.error(error);
    }
  }

  public async awaitQuery(queryExecutionId: string) {
    const input = {
      QueryExecutionId: queryExecutionId,
    };

    const command = new GetQueryExecutionCommand(input);
    const response = await this.awsAthenaInstance.send(command);

    const queryExecutionStatus = response.QueryExecution.Status.State;

    switch (queryExecutionStatus) {
      case "QUEUED":
        setTimeout(() => {
          return this.awaitQuery(queryExecutionId);
        }, 50);
        break;
      case "RUNNING":
        setTimeout(() => {
          return this.awaitQuery(queryExecutionId);
        }, 10);
        break;
      case "SUCCEEDED":
        return this.getQueryResults(queryExecutionId);
      case "FAILED":
        throw new Error();
      case "CANCELLED":
        throw new Error();
      default:
        break;
    }
  }

  public async getQueryResults(queryExecutionId: string) {
    const input = {
      QueryExecutionId: queryExecutionId,
    };

    const command = new GetQueryResultsCommand(input);
    const response = await this.awsAthenaInstance.send(command);

    return response.ResultSet.Rows;
  }
}
