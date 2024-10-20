import axios from 'axios';
import { generateBearerTokenFromSecret } from '../jwt';

export const getCatalogData = async (endpoint: string, options?: any) => {
    const { token } = await generateBearerTokenFromSecret();
    if (!options) {
        options = {};
    }
    if (!options.headers) {
        options.headers = {};
    }
    options.headers['Authorization'] = `Bearer ${token}`;
    return axios.get(endpoint, options);
};
