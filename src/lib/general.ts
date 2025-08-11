import { RegexPatterns } from "./regex";

export const NInteger = (x: any) => {
    let y = parseInt(x);
    return !Number.isNaN(y);
}


export const getDateString = (ts: number = Date.now()): string => {
    let date = new Date(ts);
    let day = date.getDate().toString();
    let month = (date.getMonth() + 1).toString();
    let year = date.getFullYear().toString();
    if (day.length == 1) {
        day = `0${day}`;
    }
    if (month.length == 1) {
        month = `0${month}`;
    }
    return `${year}-${month}-${day}`;
}

export function sanitizeSearchInput(input: string): string {
    // Normalize the string (optional, removes diacritics)
    let sanitized = input.normalize("NFKD").replace(/[\u0300-\u036f]/g, "");

    // Remove potentially harmful SQL-related characters
    sanitized = sanitized.replace(/['";`\\]/g, ""); // remove quotes, backticks, backslashes

    // Remove SQL comment markers
    sanitized = sanitized.replace(/--/g, "");

    // Allow only letters, numbers, spaces, and limited punctuation (adjust as needed)
    sanitized = sanitized.replace(/[^a-zA-Z0-9\s\-_.]/g, "");

    // Collapse multiple spaces into one
    sanitized = sanitized.replace(/\s+/g, " ");

    // Trim leading/trailing whitespace
    sanitized = sanitized.trim();

    return sanitized;
}