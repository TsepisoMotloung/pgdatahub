'use client';

import { useState, FormEvent, ChangeEvent } from 'react';
import { processData, ProcessRequest, ProcessResponse } from '@/lib/api';

interface ProcessFormProps {
  uploadData: {
    import_id: string;
    suggested_table: string;
    available_sheets: string[];
  };
  onProcessStart: () => void;
  onProcessComplete: (data: ProcessResponse) => void;
  disabled?: boolean;
}

export default function ProcessForm({
  uploadData,
  onProcessStart,
  onProcessComplete,
  disabled = false,
}: ProcessFormProps) {
  const [tableName, setTableName] = useState(uploadData.suggested_table);
  const [selectedSheet, setSelectedSheet] = useState(
    uploadData.available_sheets[0] || ''
  );
  const [primaryKeys, setPrimaryKeys] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleTableNameChange = (e: ChangeEvent<HTMLInputElement>) => {
    setTableName(e.target.value);
  };

  const handleSheetChange = (e: ChangeEvent<HTMLSelectElement>) => {
    setSelectedSheet(e.target.value);
  };

  const handlePrimaryKeysChange = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setPrimaryKeys(value ? value.split(',').map((k) => k.trim()) : []);
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!tableName.trim()) {
      setError('Please enter a table name');
      return;
    }

    if (!selectedSheet) {
      setError('Please select a sheet');
      return;
    }

    const request: ProcessRequest = {
      import_id: uploadData.import_id,
      table_name: tableName,
      selected_sheet: selectedSheet,
      primary_keys: primaryKeys.length > 0 ? primaryKeys : undefined,
    };

    setIsLoading(true);
    onProcessStart();

    try {
      const result = await processData(request);
      onProcessComplete(result);
    } catch (err: any) {
      setError(
        err.response?.data?.detail ||
          err.message ||
          'Failed to process data'
      );
      setIsLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      {/* Table Name */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Database Table Name
        </label>
        <input
          type="text"
          value={tableName}
          onChange={handleTableNameChange}
          disabled={disabled || isLoading}
          className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none transition disabled:bg-gray-100"
          placeholder="Enter table name"
        />
        <p className="text-sm text-gray-500 mt-1">
          The table will be created or updated in PostgreSQL
        </p>
      </div>

      {/* Sheet Selection */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select Sheet
        </label>
        <select
          value={selectedSheet}
          onChange={handleSheetChange}
          disabled={disabled || isLoading}
          className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none transition disabled:bg-gray-100"
        >
          {uploadData.available_sheets.map((sheet) => (
            <option key={sheet} value={sheet}>
              {sheet}
            </option>
          ))}
        </select>
        <p className="text-sm text-gray-500 mt-1">
          Sheet to import from the Excel files
        </p>
      </div>

      {/* Primary Keys (Optional) */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Primary Keys (Optional)
        </label>
        <input
          type="text"
          value={primaryKeys.join(', ')}
          onChange={handlePrimaryKeysChange}
          disabled={disabled || isLoading}
          className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none transition disabled:bg-gray-100"
          placeholder="Comma-separated column names (e.g., id, user_id)"
        />
        <p className="text-sm text-gray-500 mt-1">
          Specify columns to be primary keys (optional)
        </p>
      </div>

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
          {error}
        </div>
      )}

      <button
        type="submit"
        disabled={disabled || isLoading}
        className="w-full px-6 py-3 bg-indigo-600 text-white font-semibold rounded-lg hover:bg-indigo-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition"
      >
        {isLoading ? (
          <span className="flex items-center justify-center">
            <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Processing...
          </span>
        ) : (
          'Process Data'
        )}
      </button>
    </form>
  );
}
