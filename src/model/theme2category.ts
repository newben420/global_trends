import mapping from './theme_category_mapping.json';

const map = mapping as any;

export type CountryCode = string;
export type CategorySlug = string;

export interface RawData {
    category: CategorySlug[];
    tone: number;
    count: number;
}

export interface KeywordEntry {
    keyword: string;
    categories: CategorySlug[];
    tone: number;
    lastUpdated: number;
    count: number;
    delta: number;
}

export const theme2category = (
    themes: string[],
): string => {
    const counts: Record<string, number> = {};

    for (const theme of themes) {
        const cat = map[theme];
        if (cat) {
            counts[cat] = (counts[cat] || 0) + 1;
        }
    }

    // Find category with max count
    let majorityCategory = "others";
    let maxCount = 0;

    for (const [cat, count] of Object.entries(counts)) {
        if (count > maxCount) {
            maxCount = count;
            majorityCategory = cat;
        }
    }

    return majorityCategory;
}
