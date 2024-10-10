import { environment } from '../../environments/environment';

/**
 * requestJson is a helper function to fetch and return a Json with proper error handling
 * @param path endpoint in which to create request
 * @returns Json promise of response
 */
export async function requestJson(path: string): Promise<any> {
  // const url = new URL(path, environment.apiBaseUrl);
  const response = await fetch(`${environment.apiBaseUrl}${path}`);

  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }

  if (!response.body) {
    throw new Error('Response body is empty');
  }

  return await response.json();
}
