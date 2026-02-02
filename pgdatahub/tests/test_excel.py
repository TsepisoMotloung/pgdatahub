"""Tests for Excel file processing"""
import pytest
from pathlib import Path
import pandas as pd
from openpyxl import Workbook
from src.excel import compute_file_hash, discover_excel_files, validate_excel_file


@pytest.fixture
def temp_excel_dir(tmp_path):
    """Create temporary directory with Excel files"""
    # Create folder structure
    folder_a = tmp_path / "folder_a"
    folder_a.mkdir()
    
    folder_b = tmp_path / "folder_b" / "nested"
    folder_b.mkdir(parents=True)
    
    # Create test Excel files
    wb1 = Workbook()
    ws1 = wb1.active
    ws1.title = "Sheet1"
    ws1.append(["Name", "Age"])
    ws1.append(["Alice", 30])
    ws1.append(["Bob", 25])
    wb1.save(folder_a / "file1.xlsx")
    
    wb2 = Workbook()
    ws2 = wb2.active
    ws2.title = "Sheet1"
    ws2.append(["Product", "Price"])
    ws2.append(["Widget", 9.99])
    wb2.save(folder_b / "file2.xlsx")
    
    return tmp_path


class TestComputeFileHash:
    """Tests for file hash computation"""
    
    def test_hash_consistency(self, temp_excel_dir):
        """Test that hash is consistent for same file"""
        file_path = list(temp_excel_dir.rglob("*.xlsx"))[0]
        
        hash1 = compute_file_hash(file_path)
        hash2 = compute_file_hash(file_path)
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 produces 64 hex characters
    
    def test_different_files_different_hashes(self, temp_excel_dir):
        """Test that different files have different hashes"""
        files = list(temp_excel_dir.rglob("*.xlsx"))
        
        if len(files) >= 2:
            hash1 = compute_file_hash(files[0])
            hash2 = compute_file_hash(files[1])
            
            assert hash1 != hash2


class TestDiscoverExcelFiles:
    """Tests for Excel file discovery"""
    
    def test_discover_files(self, temp_excel_dir):
        """Test file discovery"""
        folder_files = discover_excel_files(temp_excel_dir)
        
        # Should find 2 folders
        assert len(folder_files) >= 1
        
        # Check total file count
        total_files = sum(len(files) for files in folder_files.values())
        assert total_files >= 2
    
    def test_folder_structure(self, temp_excel_dir):
        """Test folder path structure"""
        folder_files = discover_excel_files(temp_excel_dir)
        
        # Check folder parts
        folder_names = ['/'.join(parts) for parts in folder_files.keys()]
        assert any('folder_a' in name for name in folder_names)
    
    def test_nonexistent_directory(self):
        """Test handling of nonexistent directory"""
        result = discover_excel_files(Path("/nonexistent/path"))
        assert result == {}


class TestValidateExcelFile:
    """Tests for Excel file validation"""
    
    def test_valid_file(self, temp_excel_dir):
        """Test validation of valid Excel file"""
        file_path = list(temp_excel_dir.rglob("*.xlsx"))[0]
        assert validate_excel_file(file_path) is True
    
    def test_invalid_file(self, tmp_path):
        """Test validation of invalid file"""
        invalid_file = tmp_path / "not_excel.txt"
        invalid_file.write_text("This is not an Excel file")
        
        assert validate_excel_file(invalid_file) is False
