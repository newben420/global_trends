export const arrayMode = (arr: (string | number | boolean)[]) => {
    if (!arr.length) return undefined;
    const freq = arr.reduce((acc: any, v: any) => (acc[v] = (acc[v] || 0) + 1, acc), {});
    const maxCount = Math.max(...Object.values(freq) as any);
    const candidates = Object.entries(freq).filter(([, count]) => count === maxCount).map(([val]) => val);
    return candidates.length === 1 ? candidates[0] : arr[0];
};