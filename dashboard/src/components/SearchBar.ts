import { Icons } from './Icons';

export interface SearchOptions {
  searchTerm: string;
  searchType: 'all' | 'vendor' | 'po' | 'department';
}

export function renderSearchBar(onSearch: (options: SearchOptions) => void): string {
  return `
    <div class="search-container">
      <input 
        type="text" 
        id="searchInput" 
        class="search-input"
        placeholder="Search vendors, PO IDs, departments..." 
      />
      <select id="searchType" class="search-select">
        <option value="all">All</option>
        <option value="vendor">Vendors</option>
        <option value="po">PO IDs</option>
        <option value="department">Departments</option>
      </select>
      <button onclick="window.performSearch()" class="btn btn-primary">
        ${Icons.search}
        <span>Search</span>
      </button>
      <button onclick="window.clearSearch()" class="btn btn-secondary">
        Clear
      </button>
    </div>
  `;
}

export function setupSearchHandlers(onSearch: (options: SearchOptions) => void, onClear: () => void) {
  (window as any).performSearch = () => {
    const input = document.getElementById('searchInput') as HTMLInputElement;
    const type = document.getElementById('searchType') as HTMLSelectElement;
    
    if (input && type) {
      onSearch({
        searchTerm: input.value.trim(),
        searchType: type.value as SearchOptions['searchType']
      });
    }
  };
  
  (window as any).clearSearch = () => {
    const input = document.getElementById('searchInput') as HTMLInputElement;
    if (input) {
      input.value = '';
    }
    onClear();
  };
  
  // Setup enter key handler
  const input = document.getElementById('searchInput') as HTMLInputElement;
  if (input) {
    input.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        (window as any).performSearch();
      }
    });
  }
}