export const toTitleCase = (str: string): string => {
    return str
        .toLowerCase()
        .split(/\s+/) // split by any whitespace
        .map(word => {
            return word.charAt(0).toUpperCase() + word.slice(1);
        })
        .join(' ');
}
