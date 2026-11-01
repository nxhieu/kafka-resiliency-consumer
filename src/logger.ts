export type Level = "info" | "warn" | "error" | "fatal";
export type Fields = Record<string, string | number | boolean | undefined>;

export type Logger = Record<Level, (event: string, fields?: Fields) => void>;

/** One logfmt line per event, e.g. `level=warn event=message_retry type=PriceChanged attempt=1`. */
export function createLogger(write: (line: string) => void = (line) => process.stdout.write(`${line}\n`)): Logger {
  const log = (level: Level) => (event: string, fields: Fields = {}) => {
    const pairs = Object.entries({ time: new Date().toISOString(), level, event, ...fields })
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => `${key}=${format(value)}`);
    write(pairs.join(" "));
  };
  return { info: log("info"), warn: log("warn"), error: log("error"), fatal: log("fatal") };
}

export const silentLogger: Logger = createLogger(() => {});

function format(value: string | number | boolean | undefined): string {
  const text = String(value);
  return /^[\w.:/-]+$/.test(text) ? text : JSON.stringify(text);
}
