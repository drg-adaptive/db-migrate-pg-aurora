import { CallbackFunction, InternalOptions } from "db-migrate-base";
import AuroraDataApiDriver, { RDSParams } from "./AuroraDataApiDriver";

export function connect(
  config: RDSParams,
  intern: InternalOptions,
  callback: CallbackFunction,
) {
  // @ts-ignore
  callback(null, new AuroraDataApiDriver(intern, config));
}
