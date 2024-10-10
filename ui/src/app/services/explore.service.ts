import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../environments/environment';
import { requestJson } from './api-request';

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

export type Cost = {
  standard: number;
  nearline: number;
  coldline: number;
  archive: number;
}

export type Size = {
  standard: number;
  nearline: number;
  coldline: number;
  archive: number;
}
export interface SummaryResult {
  title: string;
  cost: Cost;
  size: Size;
}

@Injectable({
  providedIn: 'root',
})
export class ExploreService {
  constructor(private http: HttpClient) {}

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
    try {
      const json = await requestJson(`/explore/${path}?sort=${sort}`)

      const title = json.path as string;
      const contents = json.contents as MetadataObject[];

      return { title, contents } as ExploreResult;
    } catch (error) {
      console.error(error)
    }

    return {} as ExploreResult;
  }

  async getSummary(path: string): Promise<SummaryResult> {
    path = this.normalizePath(path);

    try {
      const json = await requestJson(`/summary/${path}`)

      const title = json.path as string;
      const cost = json.cost as Cost;
      const size = json.size as Size;

      return { title, cost, size } as SummaryResult;
    } catch (error) {
      console.error(error);
    }

    return {} as SummaryResult;
  }
}
