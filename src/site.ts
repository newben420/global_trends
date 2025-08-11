import { config } from "dotenv";
import { RegexPatterns } from "./lib/regex";
import path from "path";
const args = process.argv.slice(2);
config({
    path: args[0] || ".env",
})

export class Site {
    static TITLE: string = process.env["TITLE"] || "Title";
    static BRAND: string = process.env["BRAND"] || "Brand";
    static ROOT: string = process.cwd() || __dirname;
    static PORT: number = parseInt(process.env["PORT"] || "0") || 3000;
    static PRODUCTION = (process.env["PRODUCTION"] || "").toLowerCase() == "true";
    static FORCE_FAMILY_4 = (process.env["FORCE_FAMILY_4"] || "").toLowerCase() == "true";
    static EXIT_ON_UNCAUGHT_EXCEPTION = (process.env["EXIT_ON_UNCAUGHT_EXCEPTION"] || "").toLowerCase() == "true";
    static EXIT_ON_UNHANDLED_REJECTION = (process.env["EXIT_ON_UNHANDLED_REJECTION"] || "").toLowerCase() == "true";
    static URL = Site.PRODUCTION ? (process.env["PROD_URL"] || "") : `http://localhost:${Site.PORT}`;
    static MAX_ALLOWED_FLOG_LOG_WEIGHT: number = parseInt(process.env["MAX_ALLOWED_FLOG_LOG_WEIGHT"] || "0") ?? 5;
    
    static MAIN_INTERVAL_MS: number = parseInt(process.env["MAIN_INTERVAL_MS"] || "0") || 5;

}