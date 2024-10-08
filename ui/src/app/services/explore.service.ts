import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../environments/environment';

const API_BASE_URL = environment.apiBaseUrl;

export interface MetadataObject {
  name: string;
  count: number;
  parent: string;
  size: number;
}

export interface ExploreResult {
  title: string;
  contents: MetadataObject[];
}

export interface SummaryResult {
  bucket: string;
  title: string;
  count: number;
  size: number;
}

@Injectable({
  providedIn: 'root',
})
export class ExploreService {
  constructor(private http: HttpClient) {}

  // normalizePath normalizes value by escaping slashes
  normalizePath(path: string): string {
    if (path === '/') {
      path = '';
    } else {
      path = path.replaceAll('/', '%2f');
    }

    return path;
  }

  async getDir(path: string, sort: string): Promise<ExploreResult> {
    path = this.normalizePath(path);

    const response = await fetch(
      `${API_BASE_URL}/explore/${path}?sort=${sort}`,
    );
    if (!response.ok) {
      throw new Error(`Response status: ${response.status}`);
    }

    const json = await response.json();

    const title = json.path as string;
    const contents = json.contents as MetadataObject[];

    return { title, contents } as ExploreResult;
  }
}
