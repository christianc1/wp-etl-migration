<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Loaders;

use Flow\ETL\Adapter\WordPress\Exception\{
	WPAdapterDatabaseException,
	WPAdapterDataException,
	WPAdapterMissingDataException
};
use Flow\ETL\Adapter\WordPress\Normalizers\{
    EntryNormalizer,
    RowNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

/**
 * WordPress Media Loader for ETL operations
 *
 * This loader handles the insertion and updating of WordPress media attachments during ETL processes.
 * It supports data normalization, sanitization, and proper error handling for media operations.
 * Can handle both local file paths and remote URLs for media import.
 * Supports multiple media entries like media.featured_image.url and media.social_image.url.
 *
 * @implements Loader
 */
final class WPMediaLoader implements Loader
{
    /**
     * @var string The format to use for datetime values
     */
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    /**
     * @var array<string, mixed> Default values for media attachments
     */
    private array $mediaDefaults = [
        'post_status' => 'inherit',
        'post_type' => 'attachment',
    ];

    /**
     * Constructor
     *
     * @param array<string, mixed> $config Configuration array for the loader
     */
    public function __construct(
        private readonly array $config = []
    ) {
        $this->mediaDefaults = array_merge($this->mediaDefaults, $config['defaults'] ?? []);
    }

    /**
     * Creates a row normalizer instance
     *
     * @param FlowContext $context The flow context
     * @return RowNormalizer The created normalizer
     */
    public function create_normalizer(FlowContext $context): RowNormalizer
    {
        return new RowNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    /**
     * Loads media from the provided rows
     *
     * @param Rows $rows The rows to process
     * @param FlowContext $context The flow context
     * @throws WPAdapterMissingDataException When no media is found to process
     * @return void
     */
    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('media', 'No media found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->insertMedia($row, $normalizer);
        }
    }

    /**
     * Insert multiple media items from a single row
     *
     * @param Row|array<string, mixed> $row Row data
     * @param RowNormalizer|null $normalizer Optional normalizer
     * @return array<string, int> Array of attachment IDs keyed by media key in format 'media.{key}.attachment_id'
     * @throws WPAdapterMissingDataException When no media fields are found
     */
    public function insertMedia(Row | array $row, ?RowNormalizer $normalizer = null): array
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        // Group media fields by their prefix
        $media_groups = $this->groupMediaFields($data);

        if (empty($media_groups)) {
            throw WPAdapterMissingDataException::noEntitiesFound('media_fields', 'No media fields found in the data', [
                'available_keys' => array_keys($data)
            ]);
        }

        $results = [];
        foreach ($media_groups as $media_key => $media_data) {
            // Skip entries without URL
            if (empty($media_data['url'])) {
                continue;
            }

            // Sanitize the media data
            $media_data = $this->sanitizeMediaData($media_key, $media_data);

            try {
                $attachment_id = $this->sideloadSingleMedia($media_data);
                if ($attachment_id) {
                    // For default media (key is 'default'), use 'media.attachment_id'
                    // For specific media (e.g., 'featured_image'), use 'media.featured_image.attachment_id'
                    $result_key = ($media_key === 'default') ? 'media.attachment_id' : "media.{$media_key}.attachment_id";
                    $results[$result_key] = $attachment_id;
                }
            } catch (RuntimeException $e) {
                // Log error but continue processing other media
                error_log("Failed to process media {$media_key}: " . $e->getMessage());
            }
        }

        if (empty($results)) {
            throw WPAdapterMissingDataException::noEntitiesFound('media', 'No media was successfully processed', [
                'media_groups' => array_keys($media_groups)
            ]);
        }

        return $results;
    }

    /**
     * Group media-related fields by their prefix
     * e.g., 'media.featured_image.url' and 'media.featured_image.alt_text' are grouped together
     *
     * @param array<string, mixed> $data The normalized data
     * @return array<string, array<string, mixed>> Grouped media fields
     */
    protected function groupMediaFields(array $data): array
    {
        $media_groups = [];

        // First, check for direct media entries (media.url, media.title, etc.)
        $direct_media = [];
        foreach ($data as $key => $value) {
            if (strpos($key, 'media.') === 0 && substr_count($key, '.') === 1) {
                $field_name = substr($key, 6); // Remove 'media.'
                $direct_media[$field_name] = $value;
            }
        }

        // If we have direct media with URL, add as 'default' entry
        if (!empty($direct_media['url'])) {
            $media_groups['default'] = $direct_media;
        }

        // Process structured media entries (media.featured_image.url, etc.)
        foreach ($data as $key => $value) {
            if (strpos($key, 'media.') === 0 && substr_count($key, '.') === 2) {
                // Split the key into parts (e.g., ['media', 'featured_image', 'url'])
                $parts = explode('.', $key, 3);
                if (count($parts) !== 3) {
                    continue;
                }

                [, $media_key, $field_name] = $parts;
                $media_groups[$media_key][$field_name] = $value;
            }
        }

        return $media_groups;
    }

    /**
     * Sanitizes media data
     *
     * @param string $media_key The media key (e.g., 'featured_image')
     * @param array<string, mixed> $media_data The media data to sanitize
     * @return array<string, mixed> Sanitized media data
     */
    protected function sanitizeMediaData(string $media_key, array $media_data): array
    {
        $sanitized = [];

        // Sanitize URL
        if (isset($media_data['url'])) {
            $sanitized['url'] = esc_url_raw($media_data['url']);
        }

        // Sanitize text fields
        if (isset($media_data['title'])) {
            $sanitized['title'] = sanitize_text_field($media_data['title']);
        }

        if (isset($media_data['description'])) {
            $sanitized['description'] = wp_kses_post($media_data['description']);
        }

        if (isset($media_data['caption'])) {
            $sanitized['caption'] = sanitize_text_field($media_data['caption']);
        }

        if (isset($media_data['alt_text'])) {
            $sanitized['alt_text'] = sanitize_text_field($media_data['alt_text']);
        }

        // Sanitize post parent
        if (isset($media_data['post_parent'])) {
            $post_parent = absint($media_data['post_parent']);
            $sanitized['post_parent'] = get_post($post_parent) ? $post_parent : 0;
        }

        // Process any meta fields
        foreach ($media_data as $key => $value) {
            if (strpos($key, 'meta_') === 0) {
                $sanitized[$key] = $value; // Will be sanitized when updating meta
            }
        }

        return $sanitized;
    }

    /**
     * Handle sideloading of a single media item
     *
     * @param array<string, mixed> $media_data The media data
     * @return int The attachment ID
     * @throws WPAdapterDataException When required data is missing
     * @throws WPAdapterDatabaseException When media insertion fails
     */
    protected function sideloadSingleMedia(array $media_data): int
    {
        if (empty($media_data['url'])) {
            throw WPAdapterDataException::missingRequiredData('url', $media_data);
        }

        $url = $media_data['url'];
        $desc = $media_data['title'] ?? null;
        $post_parent = !empty($media_data['post_parent']) ? (int)$media_data['post_parent'] : 0;

        // Prepare post data for the attachment
        $post_data = array_filter([
            'post_title' => $media_data['title'] ?? '',
            'post_content' => $media_data['description'] ?? '',
            'post_excerpt' => $media_data['caption'] ?? '',
            'post_parent' => $post_parent,
        ]);

        // Download and create the attachment
        $attachment_id = $this->sideloadMedia($url, $post_parent, $desc, $post_data);

        if (is_wp_error($attachment_id)) {
            throw WPAdapterDatabaseException::fromWPError($attachment_id, "Failed to sideload media");
        }

        // Update alt text if provided
        if (!empty($media_data['alt_text'])) {
            update_post_meta($attachment_id, '_wp_attachment_image_alt', $media_data['alt_text']);
        }

        // Process any meta fields
        foreach ($media_data as $key => $value) {
            if (strpos($key, 'meta_') === 0) {
                $meta_key = substr($key, 5);
                update_post_meta($attachment_id, $meta_key, $this->sanitizeMetaValue($value));
            }
        }

        return $attachment_id;
    }

    /**
     * Sideload media from URL, checking for existing attachments first
     *
     * @param string $url The URL of the media to sideload
     * @param int $post_parent The parent post ID
     * @param string|null $desc The description
     * @param array<string, mixed> $post_data Additional post data
     * @return int|\WP_Error The attachment ID or WP_Error
     */
    protected function sideloadMedia(string $url, int $post_parent = 0, ?string $desc = null, array $post_data = []): int|\WP_Error
    {
        // Check for existing attachment by filename
        $filename = pathinfo($url, PATHINFO_FILENAME);

        // Try up to 3 variations of the filename (original, -1, -2)
        for ($i = 0; $i < 3; $i++) {
            $existing_attachment = get_posts(
                array(
                    'post_type'      => 'attachment',
                    'posts_per_page' => 1,
                    'title'          => $filename . ($i ? '-' . $i : ''),
                    'fields'         => 'ids',
                )
            );

            if (!empty($existing_attachment)) {
                $attachment_id = $existing_attachment[0];

                // Update the attachment with any new data if provided
                if (!empty($post_data)) {
                    $post_data['ID'] = $attachment_id;
                    wp_update_post($post_data);
                }

                return $attachment_id;
            }
        }

        // If no existing attachment found, proceed with sideloading
        if (!function_exists('media_sideload_image')) {
            require_once(ABSPATH . 'wp-admin/includes/media.php');
            require_once(ABSPATH . 'wp-admin/includes/file.php');
            require_once(ABSPATH . 'wp-admin/includes/image.php');
        }

        // Use media_sideload_image() with 'id' return type to get attachment ID
        $attachment_id = media_sideload_image($url, $post_parent, $desc, 'id');

        if (is_wp_error($attachment_id)) {
            return $attachment_id;
        }

        // Update the attachment post with any additional data
        if (!empty($post_data)) {
            $post_data['ID'] = $attachment_id;
            wp_update_post($post_data);
        }

        return $attachment_id;
    }

    /**
     * Sanitizes a meta value based on its type
     *
     * @param mixed $value The meta value to sanitize
     * @return mixed Sanitized meta value
     */
    private function sanitizeMetaValue($value)
    {
        if (is_numeric($value)) {
            // If it's a numeric value, preserve its type (int or float)
            return is_float($value + 0) ? (float)$value : (int)$value;
        } elseif (is_string($value)) {
            // For strings, use appropriate sanitization
            if (strpos($value, '<') !== false && strpos($value, '>') !== false) {
                // If it looks like HTML, use wp_kses_post
                return wp_kses_post($value);
            } else {
                // Otherwise use standard text sanitization
                return sanitize_text_field($value);
            }
        } elseif (is_array($value)) {
            // For arrays, recursively sanitize each element
            return array_map([$this, 'sanitizeMetaValue'], $value);
        } elseif (is_bool($value) || is_null($value)) {
            // Preserve boolean and null values
            return $value;
        } else {
            // For any other type, return as is
            return $value;
        }
    }

    /**
     * Sets the datetime format for the loader
     *
     * @param string $dateTimeFormat The format to use (should be a valid PHP date format)
     * @return self New instance with the updated format
     */
    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    /**
     * Sets default values for media fields
     *
     * @param array<string, mixed> $defaults The default values to set
     * @return self New instance with the updated defaults
     */
    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->mediaDefaults = array_merge($this->mediaDefaults, $defaults);
        return $clone;
    }
}

