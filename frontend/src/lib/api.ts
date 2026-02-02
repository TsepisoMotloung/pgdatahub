import axios, { AxiosInstance } from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

const apiClient: AxiosInstance = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export interface UploadResponse {
  import_id: string;
  suggested_table: string;
  available_sheets: string[];
}

export interface ProcessRequest {
  import_id: string;
  table_name: string;
  selected_sheet: string;
  primary_keys?: string[];
}

export interface ProcessResponse {
  success: string[];
  errors: Array<{ file: string; error: string }>;
  skipped: Array<{ file: string; reason: string }>;
}

export const uploadFile = async (file: File): Promise<UploadResponse> => {
  const formData = new FormData();
  formData.append('file', file);

  const response = await apiClient.post<UploadResponse>('/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });

  return response.data;
};

export const processData = async (
  request: ProcessRequest
): Promise<ProcessResponse> => {
  const response = await apiClient.post<ProcessResponse>('/process', request);
  return response.data;
};

export default apiClient;
