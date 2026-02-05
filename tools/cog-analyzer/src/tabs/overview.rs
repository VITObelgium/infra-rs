//! Overview tab - displays comprehensive COG file metadata.

/// State for the Overview tab.
#[derive(Debug, Default)]
pub struct OverviewTabState {
    /// Current scroll offset for the content.
    pub scroll_offset: u16,
    /// Maximum scroll offset (set during rendering).
    pub max_scroll: u16,
    /// Content height (set during rendering).
    pub content_height: u16,
}

impl OverviewTabState {
    /// Scroll up by one line.
    pub fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    /// Scroll down by one line.
    pub fn scroll_down(&mut self) {
        if self.scroll_offset < self.max_scroll {
            self.scroll_offset += 1;
        }
    }

    /// Scroll up by a page (10 lines).
    pub fn scroll_up_page(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(10);
    }

    /// Scroll down by a page (10 lines).
    pub fn scroll_down_page(&mut self) {
        self.scroll_offset = (self.scroll_offset + 10).min(self.max_scroll);
    }

    /// Scroll to the top.
    pub fn scroll_to_top(&mut self) {
        self.scroll_offset = 0;
    }

    /// Scroll to the bottom.
    pub fn scroll_to_bottom(&mut self) {
        self.scroll_offset = self.max_scroll;
    }

    /// Update the maximum scroll offset based on content and viewport height.
    pub fn update_scroll_bounds(&mut self, content_height: u16, viewport_height: u16) {
        self.content_height = content_height;
        self.max_scroll = content_height.saturating_sub(viewport_height);
        // Ensure current scroll is within bounds
        if self.scroll_offset > self.max_scroll {
            self.scroll_offset = self.max_scroll;
        }
    }
}
