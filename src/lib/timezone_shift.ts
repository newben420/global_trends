export const timezoneShift = (): number => {
    // This seems irrelevant, so
    return 0;
    const offsetInMinutes = new Date().getTimezoneOffset(); // Offset in minutes from UTC
    return offsetInMinutes * 60 * 1000; // Convert to milliseconds
};