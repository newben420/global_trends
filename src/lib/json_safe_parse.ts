export const JSONSafeParse = (str: string, isArray: boolean = false) => {
    try {
        let obj = JSON.parse(str);
        return obj;
    } catch (error) {
        return isArray ? [] : {};
    }
}