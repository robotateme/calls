<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * @property int $id
 * @property string|null $name
 * @property int|bool|string $available
 * @property int|bool|string $afk
 * @property int|null $reserved_call_id
 * @property string|null $reserved_at
 * @property string|null $last_call_at
 */
final class Operator extends Model
{
    protected $fillable = [
        'name',
        'available',
        'afk',
        'reserved_call_id',
        'reserved_at',
        'last_call_at',
    ];

}
